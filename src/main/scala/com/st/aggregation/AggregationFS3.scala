package com.st.aggregation

import cats.effect._
import cats.implicits._
import cats.effect.kernel.Resource.ExitCase
import cats.effect.kernel.{Outcome, Ref, Temporal}
import cats.effect.std.Semaphore
import fs2.{Chunk, Pipe, Stream}

import scala.concurrent.duration.FiniteDuration

object AggregationFS3 {

//  def groupWithInOld[F[_], O](maxBucketSize: Int, maxWaitTime: FiniteDuration)
//                          (implicit F: Concurrent[F], timer: Temporal[F]): Pipe[F, (O, String, Int), Chunk[O]] = ???
//                                                                                    : Stream[F2, Chunk[O]]
//                                                                                    : Pipe[F2, (O, String, Int), Chunk[O]]
  def groupWithin[F2[_], O](maxBucketSize: Int, timeout: FiniteDuration)(implicit F: Temporal[F2]): Pipe[F2, (O, String, Int), Chunk[(O, String, Int)]] = {
    case class JunctionBuffer[T](
                                  data: Vector[T],
                                  tag: Option[String],
                                  size: Int,
                                  runningSize: Long,
                                  endOfSupply: Option[Either[Throwable, Unit]],
                                  endOfDemand: Option[Either[Throwable, Unit]]
                                ) {
      def splitAt(n: Int): (JunctionBuffer[T], JunctionBuffer[T]) = {
        val size = this.size
        if (this.data.size >= n) {
          val (head, tail) = this.data.splitAt(n.toInt)
          (this.copy(tail), this.copy(head))
        } else {
          (this.copy(Vector.empty), this)
        }
      }
    }
  in =>
    fs2.Stream.force {
      for {
        demand <- Semaphore[F2](maxBucketSize.toLong)
        supply <- Semaphore[F2](0L)
        buffer <- Ref[F2].of(
          JunctionBuffer[(O, String, Int)](Vector.empty[(O, String, Int)], tag = None, size = 0, runningSize = 0L, endOfSupply = None, endOfDemand = None)
        )
      } yield {
        def enqueue(e: (O, String, Int), tag: Option[String], size: Int): F2[Boolean] =
          for {
            _ <- demand.acquire
            runningSize <- buffer.get.map(_.runningSize)
            buf <- if (runningSize + size <= maxBucketSize)
              buffer.modify(buf => (buf.copy(buf.data :+ e, tag = tag, runningSize = runningSize+size), buf))
            else
              buffer.modify(buf => (buf.copy(data = Vector.empty :+ e, tag = None, runningSize = 0L), buf))
            _ <- supply.release
          } yield buf.endOfDemand.isEmpty

        def waitN(s: Semaphore[F2]): F2[Unit] =
          F.guaranteeCase(s.acquireN(maxBucketSize.toLong)) {
            case Outcome.Succeeded(_) => s.releaseN(maxBucketSize.toLong)
            case _                    => F.unit
          }

        def acquireSupplyUpToNWithin(n: Long): F2[Long] =
          F.race(
            F.sleep(timeout),
            waitN(supply)
          ).flatMap {
            case Left(_) =>
              for {
                _ <- supply.acquire
                m <- supply.available
                k = m.min(n - 1)
                b <- supply.tryAcquireN(k)
              } yield if (b) k + 1 else 1
            case Right(_) => supply.acquireN(n) *> F.pure(n)
          }

        def dequeueN(n: Int): F2[Option[Vector[(O, String, Int)]]] =
          acquireSupplyUpToNWithin(n.toLong).flatMap { n =>
            buffer
              .modify(x => x.splitAt(n.toInt))
              .flatMap { buf =>
                demand.releaseN(buf.data.size.toLong).flatMap { _ =>
                  buf.endOfSupply match {
                    case Some(Left(error)) => F.raiseError(error)
                    case Some(Right(_)) if buf.data.isEmpty => F.pure(None)
                    case _ => F.pure(Some(buf.data))
                  }
                }
              }
          }

        def endSupply(result: Either[Throwable, Unit]): F2[Unit] =
          buffer.update(_.copy(endOfSupply = Some(result))) *> supply.releaseN(Int.MaxValue)

        def endDemand(result: Either[Throwable, Unit]): F2[Unit] =
          buffer.update(_.copy(endOfDemand = Some(result))) *> demand.releaseN(Int.MaxValue)

        val enqueueAsync: F2[Fiber[F2, Throwable, Unit]] = F.start {
          val result: F2[Unit] =
            in
            .evalMap{x => enqueue(x, Some(x._2), x._3)}
            .forall(identity)
            .onFinalizeCase {
              case ExitCase.Succeeded  => endSupply(Right(()))
              case ExitCase.Errored(e) => endSupply(Left(e))
              case ExitCase.Canceled   => endSupply(Right(()))
            }
            .compile
            .drain

          result
        }

        fs2.Stream
          .bracketCase(enqueueAsync) { case (upstream, exitCase) =>
            val ending: Either[Throwable, Unit] = exitCase match {
              case ExitCase.Succeeded  => Right(())
              case ExitCase.Errored(e) => Left(e)
              case ExitCase.Canceled   => Right(())
            }
            endDemand(ending) *> upstream.cancel
          }
          .flatMap { _ =>
            fs2.Stream
              .eval(dequeueN(maxBucketSize))
              .repeat
              .collectWhile { case Some(data) => Chunk.vector(data) }
          }
      }
    }
  }

}

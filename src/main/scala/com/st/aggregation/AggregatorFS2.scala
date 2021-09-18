package com.st.aggregation

import cats.effect._
import cats.effect.implicits._
import cats.effect.std.Queue
import cats.implicits.{catsSyntaxEither => _, _}
import fs2.{Chunk, Pipe, Pure, Stream}

import scala.concurrent.duration.FiniteDuration

object AggregatorFS2 {

  private final class Token extends Serializable {
    override def toString: String = s"Token(${hashCode.toHexString})"
  }

  def groupWithIn[F[_], O](maxBucketSize: Int, maxWaitTime: FiniteDuration)
                          (implicit F: Async[F], timer: Temporal[F]): Pipe[F, (O, String, Int), Chunk[O]] =
    in => {
      Stream
        .eval {
          Queue
            .synchronous[F, Option[Either[Token, (O, String, Int)]]]
            .product(Ref[F].of(F.unit -> false))
        }
    }
      .flatMap {
        case (queue, currentTimeout) =>
          def startTimeout: Stream[F, Token] =
            Stream
              .eval(F.delay(new Token))
              .evalTap {
                newToken =>
                  val timeout: F[Unit] = timer.sleep(maxWaitTime) >> queue.offer(newToken.asLeft.some)
                  timeout.start
                    .bracket(_ => F.unit) { fiber =>
                      currentTimeout.modify {
                        case st@(cancelInFlightTimeout, streamTerminated) =>
                          if (streamTerminated)
                            st -> fiber.cancel
                          else
                            (fiber.cancel, streamTerminated) -> cancelInFlightTimeout
                      }.flatten
                    }
              }

          def producer: Stream[F, Unit] =
            in.map(x => x.asRight.some).evalMap(queue.offer).onFinalize(queue.offer(None))
//            in.map(x => x.asRight.some).parEvalMap(10)(queue.offer).onFinalize(queue.offer(None))
//          in.map(x => x.asRight.some).parEvalMapUnordered(10)(queue.offer).onFinalize(queue.offer(None))

          def emitNonEmpty(c: Chunk.Queue[O]): Stream[F, Chunk[O]] =
            if (c.size > 0) Stream.emit(c)
            else Stream.empty

          def go(acc: Chunk.Queue[O], currentTimeout: Token, accSize: Int, accAttribute: Option[String]): Stream[F, Chunk[O]] =
            Stream.eval(queue.take).flatMap {
              case None => emitNonEmpty(acc)
              case Some(element: Either[Token, (O, String, Int)]) =>
                element match {
                  case Left(timeout) if timeout == currentTimeout =>
                    emitNonEmpty(acc) ++ startTimeout.flatMap { newTimeout =>
                      go(Chunk.Queue.empty, newTimeout, 0, None)
                    }
                  case Left(timeout) if timeout != currentTimeout => go(acc, currentTimeout, 0, None)
                  case Right(element: (O, String, Int)) =>
                    element match {
                      case (nextElement, nextAttribute, nextAttributeSize) =>
                        val runningAccumulatorAttribute: Option[String] = if (acc.size > 0) accAttribute else Some(nextAttribute)
                        val runningBucketSize: Int = accSize + nextAttributeSize

                        if (runningBucketSize < maxBucketSize && runningAccumulatorAttribute.contains(nextAttribute)) {
                          go(acc :+ Chunk(nextElement), currentTimeout, runningBucketSize, runningAccumulatorAttribute)
                        }
                        else if (runningBucketSize == maxBucketSize && runningAccumulatorAttribute.contains(nextAttribute)) {
                          Stream.empty ++ startTimeout.flatMap { newTimeout =>
                            go(acc :+ Chunk(nextElement), newTimeout, runningBucketSize, runningAccumulatorAttribute)
                          }
                        } else {
                          val stream: Stream[Pure, Chunk[O]] = if (acc.isEmpty) Stream.empty else Stream.emit(acc)
                          stream ++ startTimeout.flatMap { newTimeout =>
                            go(Chunk.Queue(Chunk(nextElement)), newTimeout, nextAttributeSize, Some(nextAttribute))
                          }
                        }
                    }
                }
            }

          startTimeout
            .flatMap(token => go(Chunk.Queue.empty, token, 0, None).concurrently(producer))
            .onFinalize {
              currentTimeout.modify {
                case (cancelInFlightTimeout, _) =>
                  (F.unit, true) -> cancelInFlightTimeout
              }.flatten
            }
      }

  def groupWithInUsingDiscriminatorFunction[F[_], O](maxBucketSize: Int, maxWaitTime: FiniteDuration)
                                                    (elementWeight: O => Long)(elementClassifier: O => String)
                                                    (implicit F: Async[F], timer: Temporal[F]): Pipe[F, O, Chunk[O]] =
    in => {
      Stream
        .eval {
          Queue
            .synchronous[F, Option[Either[Token, O]]]
            .product(Ref[F].of(F.unit -> false))
        }
    }
      .flatMap {
        case (queue, currentTimeout) =>
          def startTimeout: Stream[F, Token] =
            Stream
              .eval(F.delay(new Token))
              .evalTap {
                newToken =>
                  val timeout: F[Unit] = timer.sleep(maxWaitTime) >> queue.offer(newToken.asLeft.some)
                  timeout.start
                    .bracket(_ => F.unit) { fiber =>
                      currentTimeout.modify {
                        case st@(cancelInFlightTimeout, streamTerminated) =>
                          if (streamTerminated)
                            st -> fiber.cancel
                          else
                            (fiber.cancel, streamTerminated) -> cancelInFlightTimeout
                      }.flatten
                    }
              }

          def producer: Stream[F, Unit] =
            in.map(x => x.asRight.some).evalMap(queue.offer).onFinalize(queue.offer(None))

          def emitNonEmpty(c: Chunk.Queue[O]): Stream[F, Chunk[O]] =
            if (c.size > 0) Stream.emit(c)
            else Stream.empty

          def go(acc: Chunk.Queue[O], currentTimeout: Token, accSize: Long, accAttribute: Option[String]): Stream[F, Chunk[O]] =
            Stream.eval(queue.take).flatMap {
              case None => emitNonEmpty(acc)
              case Some(element: Either[Token, O]) =>
                element match {
                  case Left(timeout) if timeout == currentTimeout =>
                    emitNonEmpty(acc) ++ startTimeout.flatMap { newTimeout =>
                      go(Chunk.Queue.empty, newTimeout, 0, None)
                    }
                  case Left(timeout) if timeout != currentTimeout => go(acc, currentTimeout, 0, None)
                  case Right(nextElement) =>
                    val nextAttribute: String = elementClassifier(nextElement)
                    val nextAttributeSize: Long = elementWeight(nextElement)
                    val runningAccumulatorAttribute: Option[String] = if (acc.size > 0) accAttribute else Some(nextAttribute)
                    val runningBucketSize: Long = accSize + nextAttributeSize

                    if (runningBucketSize < maxBucketSize && runningAccumulatorAttribute.contains(nextAttribute)) {
                      go(acc :+ Chunk(nextElement), currentTimeout, runningBucketSize, runningAccumulatorAttribute)
                    }
                    else if (runningBucketSize == maxBucketSize && runningAccumulatorAttribute.contains(nextAttribute)) {
                      Stream.empty ++ startTimeout.flatMap { newTimeout =>
                        go(acc :+ Chunk(nextElement), newTimeout, runningBucketSize, runningAccumulatorAttribute)
                      }
                    } else {
                      val stream: Stream[Pure, Chunk[O]] = if (acc.isEmpty) Stream.empty else Stream.emit(acc)
                      stream ++ startTimeout.flatMap { newTimeout =>
                        go(Chunk.Queue(Chunk(nextElement)), newTimeout, nextAttributeSize, Some(nextAttribute))
                      }
                    }
                }
              case _ => Stream.raiseError[F](new RuntimeException("unknown error"))
            }

          startTimeout
            .flatMap(token => go(Chunk.Queue.empty, token, 0, None).concurrently(producer))
            .onFinalize {
              currentTimeout.modify {
                case (cancelInFlightTimeout, _) =>
                  (F.unit, true) -> cancelInFlightTimeout
              }.flatten
            }
      }
}

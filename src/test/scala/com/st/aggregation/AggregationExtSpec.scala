package com.st.aggregation

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.st.aggregation.AggregationFS3.groupWithin
import com.st.aggregation.AggregatorFS2.{groupWithIn, groupWithInUsingDiscriminatorFunction}
import fs2.{Chunk, Stream}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.util.Random

class AggregationLogicSpec extends AnyFunSpec with Matchers {

  implicit val runtime: IORuntime = cats.effect.unsafe.IORuntime.global
  describe("The AggregationLogic"){

    it("should aggregate by key and limit by time & size"){
      val list1 = List(Record(1, 3, "ABC", "BLACK"), Record(2, 7, "XYZ-ABC", "BLACK"), Record(3, 4, "HHHH", "BLACK"))
      val list2 = List(Record(4, 1, "Z" , "ORANGE"), Record(5, 9, "123456789", "ORANGE"), Record(6, 3, "MxN", "ORANGE"))
      val list3 = List(Record(7, 2, "EF", "BLACK"), Record(8, 8, "WZ-YZ-BC", "BLACK"))
      val input: List[Record] = list1 ++ list2 ++ list3

      val stream: Stream[IO, Record] = Stream.fromIterator[IO](input.iterator, 1024)

      val result: List[Chunk[Record]] = stream
        .map(x => (x, x.flag, x.size))
        .through(groupWithIn[IO, Record](12, 5.second))
        .compile
        .toList
        .unsafeRunSync()

      val attributes: Set[String] = result.flatMap(c => c.head.map(_.flag)).toSet
      attributes.size shouldBe 2
      result.size shouldBe 5
    }

    it("should use functions to aggregate by key and limit by time & size"){
      val record: String = Random.nextString(1024*2).mkString
      val list: Seq[String] = (0 until 6000000).map(_ => record)

      val stream: Stream[IO, String] = Stream.fromIterator[IO](list.iterator, 1024)

      val result: List[Chunk[String]] = stream
        .through(
          groupWithInUsingDiscriminatorFunction[IO, String](1000000, 59.second)(elementWeight = _ => 1L)(elementClassifier = _ => "fixed")
        )
//        .map{c => println(s"${java.time.LocalDateTime.now()} Chunk size: ${c.size}"); c}
        .compile
        .toList
        .unsafeRunSync()

      val attributes: Set[String] = result.flatMap(c => c.head).toSet
      attributes.size shouldBe 1
      result.size shouldBe 6
      result.map(_.size).sum shouldBe 6000000
    }

  }
}
case class Record(id: Int, size: Int = 0, msg: String, flag: String)
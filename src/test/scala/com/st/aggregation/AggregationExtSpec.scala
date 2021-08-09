package com.st.aggregation

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.st.aggregation.AggregationFS3.groupWithin
import com.st.aggregation.AggregatorFS2.groupWithIn
import fs2.Stream
import org.scalatest.funspec.AnyFunSpec

import scala.concurrent.duration.DurationInt

class AggregationLogicSpec extends AnyFunSpec {

  implicit val runtime: IORuntime = cats.effect.unsafe.IORuntime.global
  describe("The AggregationLogic"){

    it("should chunk by key and limit"){
      val list1 = List(Record(1, 3, "ABC", "BLACK"), Record(2, 7, "XYZ-ABC", "BLACK"), Record(3, 4, "HHHH", "BLACK"))
      val list2 = List(Record(4, 1, "Z" , "ORANGE"), Record(5, 9, "123456789", "ORANGE"), Record(6, 3, "MxN", "ORANGE"))
      val list3 = List(Record(7, 2, "EF", "BLACK"), Record(8, 8, "WZ-YZ-BC", "BLACK"))
      val input = list1 ++ list2 ++ list3

      val stream = Stream.fromIterator[IO](input.iterator, 1024)

      val result = stream
        .map(x => (x, x.flag, x.size))
//        .through(groupWithin[IO, Record](12, 5.second))
        .through(groupWithIn[IO, Record](12, 5.second))
        .map{c => println(c); c}
        .compile
        .toList
        .unsafeRunSync()

      println(s"Total chunks: ${result.size}")
      println(s"Result: $result")
    }

  }
}
case class Record(id: Int, size: Int = 0, msg: String, flag: String)
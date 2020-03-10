package com.lp.test.state

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.operators.KeyFunctions
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.{KeySelectorWithType, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object mapwithstate {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.socketTextStream("47.52.108.102", 9000)

    val stream = ds.map { one =>
      val split = one.split(" ")
      (split(0), split(1).toInt)
    }

    val ds3 = stream.keyBy(0).mapWithState { (in: (String, Int), count: Option[Int]) =>
      count match {
        case Some(c) => ((in._1, c), Some(c + in._2))
        //case None => ((in._1, 0), Some(in._2))
        case None => ((in._1, 0), Some(in._2))
      }
    }
    println(ds3.getClass)
    println(ds3.getType())
    ds3.print()

    env.execute("dsf")

  }
}

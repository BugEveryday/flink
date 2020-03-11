package com.test.trans

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object S01_Map {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[Int] = env.fromCollection(List(1,2,3,4,5))

    val mapDS: DataStream[Int] = ds.map(_*2)

    mapDS.print("map*2=")

    env.execute("map")
  }
}

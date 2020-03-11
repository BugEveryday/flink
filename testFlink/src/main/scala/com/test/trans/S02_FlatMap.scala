package com.test.trans


import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object S02_FlatMap {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[Int] = env.fromCollection(List(1,2,3,4,5))

    val ds2: DataStream[String] = env.fromCollection(List("1,2","2,3"))

    val flatMap1: DataStream[Int] = ds.flatMap(t=>List(t*2))

    flatMap1.print("flatMap1:")

    ds2.flatMap(_.split(","))

    env.execute("map")
  }
}

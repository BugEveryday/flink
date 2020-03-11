package com.test.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import org.apache.flink.api.scala._

object FromCollection {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[Int] = env.fromCollection(List(1,2,3,4))

    ds.print()
    
    env.execute("collection")

  }

}

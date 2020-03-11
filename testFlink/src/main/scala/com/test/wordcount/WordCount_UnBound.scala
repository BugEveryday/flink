package com.test.wordcount

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object WordCount_UnBound {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val inputDS: DataStream[String] = env.socketTextStream("hadoop222",999)

//    val result: DataStream[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_,1)).keyBy(_._1).sum(1)
    val flatDS: DataStream[String] = inputDS.flatMap(_.split(" "))
    val mapDS: DataStream[(String, Int)] = flatDS.map((_,1))
    val keyByDS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)
    val result: DataStream[(String, Int)] = keyByDS.sum(1)

    result.print()
    keyByDS.print()

    env.execute()

  }

}

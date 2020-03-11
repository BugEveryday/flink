package com.test.window

import org.apache.flink.api.common.functions.{AggregateFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}

object WindowFunction_Aggregate {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[String] = env.socketTextStream("localhost", 999)

    val wordcount: DataStream[Int] = ds.flatMap(_.split(" ")).map((_, 1)).keyBy(0)
      .countWindow(3)
      .aggregate(new AggregateFunction[(String, Int), Int, Int] {
        // 将输入值对累加器进行更新
        override def add(value: (String, Int), accumulator: Int): Int = value._2 + accumulator
        // 创建累加器
        override def createAccumulator(): Int = 0
        // 获取累加器的值
        override def getResult(accumulator: Int): Int = accumulator
        // 合并累加器的值
        override def merge(a: Int, b: Int): Int = a + b
      })

    wordcount.print()

    env.execute()
  }
}

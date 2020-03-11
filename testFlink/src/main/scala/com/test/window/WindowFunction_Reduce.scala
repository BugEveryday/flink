package com.test.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment,_}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowFunction_Reduce {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[String] = env.socketTextStream("localhost",999)

    val wordcount: DataStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_,1)).keyBy(0)
      .timeWindow(Time.seconds(3))
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1,value1._2+value2._2)
        }
      })

    wordcount.print()

    env.execute()
  }
}

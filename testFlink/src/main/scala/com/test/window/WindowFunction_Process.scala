package com.test.window

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

object WindowFunction_Process {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[String] = env.socketTextStream("localhost",999)

//    ds.flatMap(_.split(" ")).map((_,1)).keyBy(0)
//      .timeWindow(Time.seconds(2))
//      .process(new MyProcessFunction)

//    wordcount.print()

    env.execute()
  }
  class MyProcessFunction extends ProcessWindowFunction[(String,Int),String,String,GlobalWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
      val l: Long = context.window.maxTimestamp()
      out.collect(elements.toList.toString())
    }
  }
}

package com.test.window

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TimeWindow {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[String] = env.socketTextStream("localhost",999)

    val wordcount: DataStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_,1)).keyBy(0)
      .timeWindow(Time.seconds(5))
      .reduce((a1,a2)=>(a1._1,a1._2+a2._2))

    ds.timeWindowAll(Time.seconds(2)).apply(
      (widow,in,out:Collector[String])=>{
        val start: Long = widow.getStart
        for (elem <- in) {out.collect(start+":"+elem*2)}
        out
      }
    ).print()

    //SlidingWindow
//    val wordcount: DataStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_,1)).keyBy(0)
//      .timeWindow(Time.seconds(5),Time.seconds(2))
//      .reduce((a1,a2)=>(a1._1,a1._2+a2._2))

//    wordcount.print()

    env.execute()
  }
}

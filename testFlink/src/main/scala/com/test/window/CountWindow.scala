package com.test.window

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object CountWindow {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[String] = env.socketTextStream("localhost",999)

    val wordcount: DataStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_,1)).keyBy(0)
      .countWindow(3)//同样的元素出现3次，才会计算，没有达到的保留
      .reduce((t1,t2)=>(t1._1,t1._2+t2._2))

//    val wordcount: DataStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_,1)).keyBy(0)
//      .countWindow(3,2)//第二个参数是同样元素出现的次数，达到就开始计算
//      //就是3个元素中，两个元素是一样的，就会计算，但最多是3，没有达到的会保留
//      .reduce((t1,t2)=>(t1._1,t1._2+t2._2))

    wordcount.print()

    env.execute()
  }
}

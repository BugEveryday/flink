package com.test.time.eventTime

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TumblingEventTime {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val ds: DataStream[String] = env.socketTextStream("localhost", 999)

    val wsDS: DataStream[String] = ds.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(2)) {
        override def extractTimestamp(element: String): Long = {
          System.currentTimeMillis()
        }
      }
    )

    val wcDS: DataStream[(String, Int)] = wsDS.map((_,1)).keyBy(_._1)
      .window(//.timeWindow(Time.seconds(3))和这种没什么区别
        TumblingEventTimeWindows.of(Time.seconds(2))
      )
      .reduce((a1, a2) => (a1._1, a1._2 + a2._2))

    wcDS.print()

    env.execute()
  }

}

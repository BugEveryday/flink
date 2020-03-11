package com.test.time

import com.test.source.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment,_}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Periodic {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //每隔3秒产生一个新的watermark
    env.getConfig.setAutoWatermarkInterval(3000)

    val ds: DataStream[String] = env.socketTextStream("localhost",999)
    val sensorDS: DataStream[WaterSensor] = ds.map(
      data => {
        val datas = data.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
      }
    )

    //    对于有序数据
        val wartermarkDS: DataStream[WaterSensor] = sensorDS.assignAscendingTimestamps(ele=>ele.ts*1000)
    //    对于乱序数据
//    val wartermarkDS: DataStream[WaterSensor] = sensorDS.assignTimestampsAndWatermarks(
//      new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
//        override def extractTimestamp(element: WaterSensor): Long = {
//          element.ts * 1000
//        }
//      }
//    )
    val applyDS: DataStream[String] = wartermarkDS.keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .apply(
        // WindowFunction[T, R, K, W] window函数需要的参数都是in out key window
        (key: String, window: TimeWindow, in: Iterable[WaterSensor], out: Collector[String]) => {
          val inter: Long = window.getStart - window.getEnd
          out.collect(inter + "ms -- " + in)
        })

    wartermarkDS.print("wartermark")
    applyDS.print("window")

    env.execute()


  }
}

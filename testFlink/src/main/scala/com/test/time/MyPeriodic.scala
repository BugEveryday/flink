package com.test.time

import com.test.source.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark

object MyPeriodic {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //每5秒调用getCurrentWatermark()方法
    env.getConfig.setAutoWatermarkInterval(5000)

    val ds: DataStream[String] = env.socketTextStream("localhost", 999)
    val sensorDS: DataStream[WaterSensor] = ds.map(
      data => {
        val datas = data.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
      }
    )

//周期性的生成watermark：系统会周期性的将watermark插入到流中(水位线也是一种特殊的事件数据)。默认周期是200毫秒
    val wartermarkDS: DataStream[WaterSensor] = sensorDS.assignTimestampsAndWatermarks(
      new AssignerWithPeriodicWatermarks[WaterSensor] {
        var curWm = 0L
        //更新watermark
        override def getCurrentWatermark: Watermark = {
          //会周期性地获取水位线数据
          println("wartermark")
          new Watermark(curWm-3000)//比应该的时间小3秒的时候触发操作，也就是当前时间是应该时间+3秒
        }

        //指定时间戳
        override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
          curWm=curWm.max(element.ts*1000L)
          element.ts*1000L
        }
      }
    )


    wartermarkDS.print("wartermark")

    env.execute()


  }
}

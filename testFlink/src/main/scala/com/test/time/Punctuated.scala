package com.test.time

import com.test.source.WaterSensor
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment,_}
import org.apache.flink.streaming.api.watermark.Watermark

object Punctuated {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[String] = env.socketTextStream("localhost", 999)

    val sensorDS: DataStream[WaterSensor] = ds.map(
      data => {
        val datas = data.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
      }
    )
//间断生成watermark，周期性的要不断创建对象，耗性能
    val punctuatedDS: DataStream[WaterSensor] = sensorDS.assignTimestampsAndWatermarks(
      new AssignerWithPunctuatedWatermarks[WaterSensor] {
        override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
          new Watermark(extractedTimestamp)
        }

        override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
          element.ts * 1000
        }
      }
    )
    punctuatedDS.print()

    env.execute()

  }

}

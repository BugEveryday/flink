package com.test.exercise

import com.test.source.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment,_}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 5秒内水位连续上升就报警，并将高于5的输出到侧输出流
  */
object WaterSensorExe {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val ds: DataStream[String] = env.socketTextStream("localhost",999)

    val wsDS: DataStream[WaterSensor] = ds.map(
      data => {
        val line: Array[String] = data.split(",")
        WaterSensor(line(0), line(1).toInt, line(2).toLong)
      }
    )
    val timeWSDS: DataStream[WaterSensor] = wsDS.assignTimestampsAndWatermarks(
      new AssignerWithPunctuatedWatermarks[WaterSensor] {override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
        new Watermark(extractedTimestamp)
      }

        override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
          element.ts*1000L
        }
      }
    )
    val hight = new  OutputTag[Double]("hight")
//
    val processDS: DataStream[String] = timeWSDS.keyBy(_.id).process(
      new KeyedProcessFunction[String, WaterSensor, String] {
        //      当前水位
        private var currentHight = 0L
        //      警报时间
        private var alertTime = 0L

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
          out.collect(ctx.getCurrentKey+"----"+timestamp)
        }

        override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {

          if (value.vc > currentHight) {
            //如果高于currentHight，alertTime就更新，并注册
            alertTime = ctx.timestamp() + 5000L
            ctx.timerService().registerEventTimeTimer(alertTime)
          } else {
            //否则，删除之前的alertTime，更新并注册
            ctx.timerService().deleteEventTimeTimer(alertTime)
            alertTime = ctx.timestamp() + 5000L
            ctx.timerService().registerEventTimeTimer(alertTime)
          }
          currentHight = value.vc.toLong

//          将高于5的输出。侧输出流
          if(value.vc>5){
            ctx.output(hight,value.vc)
          }else{
//          不高于
            out.collect(value.vc +"不高于")
          }

        }

      }
    )
    wsDS.print("water>>>")
    processDS.print("process>>>>")
    val sideoutputDS: DataStream[Double] = processDS.getSideOutput(hight)
    sideoutputDS.print("side>>>>")

    env.execute()
  }

}

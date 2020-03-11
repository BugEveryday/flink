package com.test.processFunction

import java.lang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object Keyed {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
// 如果没有设置为eventtime，且赋值，那么函数里面的timestamp就是null
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val ds: DataStream[String] = env.socketTextStream("localhost",999)

    val timeDS: DataStream[(String, Int)] = ds.map((_, 1)).assignAscendingTimestamps(a => System.currentTimeMillis())

    val keyedDS: DataStream[String] = timeDS.keyBy(_._1)
      .process(
        new KeyedProcessFunction[String, (String, Int), String] {
          // 定时器，在指定时间点触发该方法的执行
          override def onTimer(
            timestamp: Long,
            ctx: KeyedProcessFunction[String,(String, Int), String]#OnTimerContext,
            out: Collector[String]): Unit = {

            out.collect("onTimer"+System.currentTimeMillis())
          }
          // 每来一条数据，方法会触发执行一次
          override def processElement(
             value: (String, Int), //输入
             ctx: KeyedProcessFunction[String, (String, Int), String]#Context, //环境
             out: Collector[String] //输出
           ): Unit = {
            //ctx.getCurrentKey // 获取当前数据的key
            //ctx.output() // 采集数据到侧输出流
            //ctx.timestamp() // 时间戳(事件),需要设置eventtime
            //ctx.timerService() // 和时间相关的服务（定时器）
            //ctx.timerService().currentProcessingTime()//处理时间
            //ctx.timerService().currentWatermark()
            //ctx.timerService().registerEventTimeTimer()
            //ctx.timerService().registerProcessingTimeTimer()
            //ctx.timerService().deleteEventTimeTimer()
            //ctx.timerService().deleteProcessingTimeTimer()
            //getRuntimeContext // 运行环境
            val currentProcessing: Long = ctx.timerService().currentProcessingTime()

            val timestamp: lang.Long = ctx.timestamp()
            println("currentProcessing=" + currentProcessing + "," + "timestamp=" + timestamp)
            out.collect("prefix" + value._1)

//            注册定时器
            ctx.timerService().registerEventTimeTimer(currentProcessing+1000)//会在处理数据之后1s运行onTimer方法

          }
        }
      )

    keyedDS.print()

    env.execute()

  }
}

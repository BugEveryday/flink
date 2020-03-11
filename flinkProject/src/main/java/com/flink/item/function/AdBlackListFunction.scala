package com.flink.item.function

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * 对每一条数据进行累加，如果大于等于100，就是黑名单
  * 到第二天解禁
  */
class AdBlackListFunction extends KeyedProcessFunction[String, (String, Long), (String, Long)] {
  var count: ValueState[Long] = _
  var alarm: ValueState[Boolean] = _

  override def open(parameters: Configuration): Unit = {
    count = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("count", classOf[Long])
    )
    alarm = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("alarm", classOf[Boolean])
    )
  }

  override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
    val oldCount: Long = count.value()

    val newCount: Long = oldCount + 1
    //到达100，或者alarm标记是true的时候
    if (newCount >= 100 || !alarm.value()) {
      //搞个侧输出，和定时器
      //侧输出
      val outputTag = new OutputTag[(String, Long)]("blackList")
      ctx.output(outputTag, value)
      //更新一下alarm
      alarm.update(true)
      //以这次的时间戳作为初始值，做个定时器（到第二天清除count和alarm
      val currentTime: Long = ctx.timerService().currentProcessingTime()
      val currentDay: Long = currentTime / 1000 / 60 / 60 / 24
      val nextDay: Long = currentDay + 1
      val nextTime: Long = nextDay * 24 * 60 * 60 * 1000

      ctx.timerService().registerProcessingTimeTimer(nextTime)

    }else{
      //否则就输出
      out.collect(value)
    }

    //更新一下count
    count.update(newCount)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {
    count.clear()
    alarm.clear()
  }
}

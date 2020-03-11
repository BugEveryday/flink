package com.flink.item.function

import java.lang
import java.sql.Timestamp

import com.flink.item.TopNUrlClick
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class TopNUrlKeyedProcessFunction extends KeyedProcessFunction[Long, TopNUrlClick, String] {

  private var resourceList: ListState[TopNUrlClick] = _
  private var alarmTimer: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    resourceList = getRuntimeContext.getListState(
      new ListStateDescriptor[TopNUrlClick]("resourceList", classOf[TopNUrlClick])
    )
    alarmTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("alarmTimer", classOf[Long])
    )
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, TopNUrlClick, String]#OnTimerContext, out: Collector[String]): Unit = {
    val datas: lang.Iterable[TopNUrlClick] = resourceList.get()
    val list = new ListBuffer[TopNUrlClick]
    import scala.collection.JavaConversions._
    for (data <- datas) {
      list.add(data)
    }

    // 清除状态数据
    resourceList.clear()
    alarmTimer.clear()

    val result: ListBuffer[TopNUrlClick] = list.sortWith(
      (left, right) => {
        left.clickCount > right.clickCount
      }
    ).take(3)

    val builder = new StringBuilder
    builder.append("当前时间：" + new Timestamp(timestamp) + "\n")
    for (data <- result) {
      builder.append("URL：" + data.url + ", 点击数量：" + data.clickCount + "\n")
    }
    builder.append("================")

    out.collect(builder.toString())
  }

  override def processElement(value: TopNUrlClick, ctx: KeyedProcessFunction[Long, TopNUrlClick, String]#Context, out: Collector[String]): Unit = {
    // 保存数据的状态
    resourceList.add(value)
    // 设置定时器
    if (alarmTimer.value() == 0) {
      ctx.timerService().registerEventTimeTimer(value.windowEndTime)
      alarmTimer.update(value.windowEndTime)
    }
  }

}

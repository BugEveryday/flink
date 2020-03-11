package com.flink.item.function

import java.{lang, util}
import java.sql.Timestamp

import com.flink.item.bean.TopNClick
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/*
 * 对每一条数据进行处理
 * 但是需要分窗，所以就用onTimer，时间到了触发
 * 因为有之前的状态需要暂存，所以就用有状态的变量
 */
class TopNClickKeyedProcessFunction extends KeyedProcessFunction[Long, TopNClick, String] {
  //  用来暂存同一个窗口的数据
  var list: ListState[TopNClick] = _
  //  定时器
  var alarmTimer: ValueState[Long] = _

  //  初始化
  override def open(parameters: Configuration): Unit = {

    list = getRuntimeContext.getListState(new ListStateDescriptor[TopNClick]("list", classOf[TopNClick]))

    alarmTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("alarmTimer", classOf[Long]))
  }

  override def processElement(value: TopNClick, ctx: KeyedProcessFunction[Long, TopNClick, String]#Context, out: Collector[String]): Unit = {
    //    添加数据
    list.add(value)
    //    注册定时器
    if (alarmTimer.value() == 0) {
      ctx.timerService().registerEventTimeTimer(value.windowEndTime)

      alarmTimer.update(value.windowEndTime)
    }

  }
//  时间到就触发
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, TopNClick, String]#OnTimerContext, out: Collector[String]): Unit = {

//    先把暂存的数据取出来
    val clicks: lang.Iterable[TopNClick] = list.get()
//    因为要排序，所以要List
    val listBuffer = new ListBuffer[TopNClick]

    val clicksIter: util.Iterator[TopNClick] = clicks.iterator()
    while (clicksIter.hasNext){
      listBuffer.append(clicksIter.next())
    }
    //一个窗口搞完，状态也没有用了，清掉
    list.clear()
    alarmTimer.clear()

//    取出前三的数据
    val topNList: ListBuffer[TopNClick] = listBuffer.sortBy(_.clickCount)(Ordering.Long.reverse).take(3)

//    输出
    val builder = new StringBuilder
    builder.append("当前时间：" + new Timestamp(timestamp) + "\n")
    for ( data <- topNList ) {
      builder.append("商品：" + data.itemId + ", 点击数量：" + data.clickCount + "\n")
    }
    builder.append("================")

    out.collect(builder.toString())

  }
}

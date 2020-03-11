package com.flink.item.service

import com.flink.item.bean
import com.flink.item.common.{TDao, TService}
import com.flink.item.dao.TopNClickDao
import com.flink.item.function.{TopNClickAggregateFunction, TopNClickKeyedProcessFunction, TopNClickWindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class TopNClickService extends TService {

  private val dao = new TopNClickDao

  override def getDao(): TDao = dao

  override def analyses() = {
    val ds: DataStream[bean.UserBehavior] = getUserBehaviorDatas()

    //    设置waterm
    val timeDS: DataStream[bean.UserBehavior] = ds.assignAscendingTimestamps(_.timestamp * 1000L)

    //    数据清洗，只保留点击的数据
    val filterDS: DataStream[bean.UserBehavior] = timeDS.filter(_.behavior == "pv")

    //  要开窗，先要keyBy，分类
    val keyedDS: KeyedStream[bean.UserBehavior, Long] = filterDS.keyBy(_.itemId)

    //    开窗，设置窗口
    val windowDS: WindowedStream[bean.UserBehavior, Long, TimeWindow] = keyedDS.timeWindow(Time.hours(1), Time.minutes(5))

    //    对窗口内的数据进行汇总
    // 6.2 将聚合的结果进行转换，方便排序
    // aggregate函数需要两个参数
    // 第一个参数表示聚合函数
    // 第二个参数表示当前窗口的处理函数
    // 第一个函数的处理结果会作为第二个函数输入值进行传递

    // 当窗口数据进行聚合后，会将所有窗口数据全部打乱，获取总的数据
    val topNClickDS: DataStream[bean.TopNClick] = windowDS.aggregate(
      new TopNClickAggregateFunction,
      new TopNClickWindowFunction)

    //    所以重新分组，区分不同的窗口
    val keyedKS: KeyedStream[bean.TopNClick, Long] = topNClickDS.keyBy(_.windowEndTime)

    val result: DataStream[String] = keyedKS.process(new TopNClickKeyedProcessFunction)

    result
  }


}

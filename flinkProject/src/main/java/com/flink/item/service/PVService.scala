package com.flink.item.service

import com.flink.item.bean
import com.flink.item.common.{TDao, TService}
import com.flink.item.dao.PVDao
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream,_}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 求pv，其实就是行为是pv的wordcount
  */
class PVService extends TService{
  override def getDao(): TDao = new PVDao

  override def analyses() = {

    //1 数据源，并封装样例类
    val ds: DataStream[bean.UserBehavior] = getUserBehaviorDatas()
    //2 指定时间戳和水位线
    val timeDS: DataStream[bean.UserBehavior] = ds.assignAscendingTimestamps(_.timestamp*1000L)
    //3 过滤，只保留pv
    val filteredDS: DataStream[bean.UserBehavior] = timeDS.filter(_.behavior=="pv")
    //4 将过滤后的数据进行结构的转换 ： （pv, 1）
    val pvToOneDS: DataStream[(String, Int)] = filteredDS.map(pv=>("pv", 1))
    //5将相同的key进行分组
    val pvToOneKS: KeyedStream[(String, Int), String] = pvToOneDS.keyBy(_._1)
    //6 设定窗口范围
    val pvToOneWS: WindowedStream[(String, Int), String, TimeWindow] = pvToOneKS.timeWindow(Time.hours(1))
    //7 数据聚合
    val pvToSumDS: DataStream[(String, Int)] = pvToOneWS.sum(1)

    pvToSumDS

  }
}

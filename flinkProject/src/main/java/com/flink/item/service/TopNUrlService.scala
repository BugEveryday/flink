package com.flink.item.service

import java.text.SimpleDateFormat

import com.flink.item.{ApacheLog, TopNUrlClick}
import com.flink.item.common.{TDao, TService}
import com.flink.item.dao.TopNUrlDao
import com.flink.item.function.{TopNUrlAggregateFunction, TopNUrlKeyedProcessFunction, TopNUrlWindowFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream,_}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class TopNUrlService extends TService {
  override def getDao(): TDao = new TopNUrlDao

  override def analyses() = {
    //1 获取数据
    val ds: DataStream[String] = getDao().readTextFile("input/apache.log")
    //2 封装数据到样例类
    val logDS: DataStream[ApacheLog] = ds.map(
      log => {
        val datas: Array[String] = log.split(" ")
        val sdf = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")
        ApacheLog(
          datas(0),
          datas(1),
          sdf.parse(datas(3)).getTime,//转为long，时间戳
          datas(5),
          datas(6)
        )
      }
    )
    //3 分配时间戳和水位线
    val waterDS: DataStream[ApacheLog] = logDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.minutes(1)) {
        override def extractTimestamp(element: ApacheLog): Long = {
          element.eventTime
        }
      }
    )

    //4 按照URL分组
    val logKS: KeyedStream[ApacheLog, String] = waterDS.keyBy(_.url)

    //5 分组求和，进行聚合
    val logWS: WindowedStream[ApacheLog, String, TimeWindow] =
      logKS.timeWindow(Time.minutes(10), Time.seconds(5))

    val aggDS: DataStream[TopNUrlClick] = logWS.aggregate(
      new TopNUrlAggregateFunction,
      new TopNUrlWindowFunction
    )
    //6 聚合后，所有数据会混到一起，要重新分组
    val aggKS: KeyedStream[TopNUrlClick, Long] = aggDS.keyBy(_.windowEndTime)

    //7 分组之后的数据进行处理
    val result: DataStream[String] = aggKS.process(new TopNUrlKeyedProcessFunction)
//    result.print()
    result
  }
}

package com.flink.item.service

import com.flink.item.MarketingUserBehavior
import com.flink.item.common.{TDao, TService}
import com.flink.item.dao.AppMarketDAO
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 对不同应用商店
  * "AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba"
  * 中app的
  * "BROWSE", "CLICK", "PURCHASE", "UNINSTALL"
  * 行为进行统计。
  * userid_行为_商店
  */
class AppMarketService extends TService {
  private val dao = new AppMarketDAO

  override def getDao(): TDao = dao

  override def analyses() = {
    //1 获取数据，已经封装好了
    val ds: DataStream[MarketingUserBehavior] = getDao().readMySource()

    //2 时间戳和水位线
    val timeDS: DataStream[MarketingUserBehavior] = ds.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[MarketingUserBehavior](Time.seconds(5)) {
        override def extractTimestamp(element: MarketingUserBehavior): Long = {
          element.timestamp
        }
      }
    )

    //3 变形
    val mapDS: DataStream[(String, Int)] = timeDS.map(
      o => {
        (o.channel + "_" + o.behavior, 1)
      }
    )
    mapDS
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(5))
      // 求和，但是返回的结果其实就是 (word, totalcount)
      //.sum()
      // 求和，可以自定义输出内容，包括窗口信息
      .process(
      new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
          out.collect(context.window.getStart + " - " + context.window.getEnd +":"+elements.iterator.next()._1+ ",APP安装量 = " + elements.size)
        }
      }
    )

  }
}

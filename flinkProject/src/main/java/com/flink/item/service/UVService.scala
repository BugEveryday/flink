package com.flink.item.service

import com.flink.item.bean
import com.flink.item.common.{TDao, TService}
import com.flink.item.dao.UVDao
import com.flink.item.function.{UVBloomFilterWindowFunction}
import org.apache.flink.streaming.api.scala.{AllWindowedStream, DataStream,_}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class UVService extends TService {
  override def getDao(): TDao = new UVDao

  override def analyses() = {
    //1 获取数据源，并封装样例类
    val ds: DataStream[bean.UserBehavior] = getUserBehaviorDatas()
    //2 时间语义
    val timeDS: DataStream[bean.UserBehavior] = ds.assignAscendingTimestamps(_.timestamp * 1000L)
    //3 转换结构为（id，1）
    val userDS: DataStream[(Long, Int)] = timeDS.map(data => (data.userId, 1))
    //分组其实毫无意义，就不分组了
    //4 开窗。将1h内所有的数据都放进去
    val windowAWS: AllWindowedStream[(Long, Int), TimeWindow] = userDS.timeWindowAll(Time.hours(1))
    //5 对窗里面的数据进行计算
    // 不能使用全量数据处理，因为会将窗口所有数据放置在内存中
    //dataWS.process()
    // 不能使用累加器，因为累加器一般只应用于单独数据累加，不做业务逻辑处理
    //dataWS.aggregate()

    // 希望能够一个一个数据进行业务逻辑处理
    // 可以使用window的触发器
    windowAWS.trigger(
      new Trigger[(Long, Int), TimeWindow]() {
        override def onElement(element: (Long, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
          // 开始计算,计算完毕后，将数据从窗口中清除
          TriggerResult.FIRE_AND_PURGE
        }

        override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
          TriggerResult.CONTINUE
        }

        override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
          TriggerResult.CONTINUE
        }

        override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {

        }
      }
    ).process(
      new UVBloomFilterWindowFunction
    )
  }
}

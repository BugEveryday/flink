package com.flink.item.service

import com.flink.item.OrderEvent
import com.flink.item.common.{TDao, TService}
import com.flink.item.dao.OrderTimeOutDao
import com.flink.item.function.OrderTimeOutKeyedProcessFunciton
import com.flink.item.util.FlinkStreamEnv
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, _}
import org.apache.flink.streaming.api.windowing.time.Time

class OrderTimeOutService extends TService {
  private val dao = new OrderTimeOutDao

  override def getDao(): TDao = dao

  override def analyses() = {
    //    analysesWithCEP()
//获取数据
    val ds: DataStream[String] = FlinkStreamEnv.get().readTextFile("input/OrderLog.csv")
//封装
    val eventDS: DataStream[OrderEvent] = ds.map {
      data => {
        val dates: Array[String] = data.split(",")
        OrderEvent(
          dates(0).toLong,
          dates(1),
          dates(2),
          dates(3).toLong
        )
      }
    }
//时间戳和水位线
    val timeDS: DataStream[OrderEvent] = eventDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(5)) {
        override def extractTimestamp(element: OrderEvent): Long = {
          element.eventTime * 1000L
        }
      }
    )
//keyed
    val keyedKS: KeyedStream[OrderEvent, Long] = timeDS.keyBy(_.orderId)

//    处理。create和pay可能有延迟
    keyedKS.process(
      new OrderTimeOutKeyedProcessFunciton
    )

  }

  // 可能会有缺失数据的存在：只有create或pay，或者顺序颠倒了
  //  所以这种方法的结果并不十分准确
  def analysesWithCEP() = {
    val ds: DataStream[String] = FlinkStreamEnv.get().readTextFile("input/OrderLog.csv")

    val eventDS: DataStream[OrderEvent] = ds.map {
      data => {
        val dates: Array[String] = data.split(",")
        OrderEvent(
          dates(0).toLong,
          dates(1),
          dates(2),
          dates(3).toLong
        )
      }
    }

    val timeDS: DataStream[OrderEvent] = eventDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(5)) {
        override def extractTimestamp(element: OrderEvent): Long = {
          element.eventTime * 1000L
        }
      }
    )
    val keyedKS: KeyedStream[OrderEvent, Long] = timeDS.keyBy(_.orderId)

    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("followedBy")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    val reslutPS: PatternStream[OrderEvent] = CEP.pattern(keyedKS, pattern)

    // within可以在指定时间范围内定义规则。
    // select方法支持函数柯里化
    // 第一个参数列表表示侧输出流标签
    // 第二个参数列表表示超时数据的处理
    // 第三个参数列表表示正常数据的处理
    val tag = new OutputTag[String]("order")
    val selectDS: DataStream[String] = reslutPS.select(
      tag
    )(
      (map, ts) => {
        map.toString()
      }
    )(
      map => {
        val order: OrderEvent = map("begin").iterator.next()
        val pay: OrderEvent = map("followedBy").iterator.next()
        var s = "订单ID :" + order.orderId
        s += "共耗时 " + (pay.eventTime - order.eventTime) + " 秒"
        s
      }
    )
    selectDS.getSideOutput(tag)
  }
}

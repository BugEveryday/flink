package com.flink.item.service

import com.flink.item.LoginEvent
import com.flink.item.common.{TDao, TService}
import com.flink.item.dao.LoginFailDao
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time

class LoginFailService extends TService {
  private val dao = new LoginFailDao

  override def getDao(): TDao = dao

  override def analyses() = {

    //1 数据
    val ds: DataStream[String] = getDao().readTextFile("input/LoginLog.csv")
    //2 封装
    val loginDS: DataStream[LoginEvent] = ds.map(
      data => {
        val datas = data.split(",")
        LoginEvent(
          datas(0).toLong,
          datas(1),
          datas(2),
          datas(3).toLong
        )
      }
    )
    //3 时间戳和水位线
    val timeDS: DataStream[LoginEvent] = loginDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(10)) {
        override def extractTimestamp(element: LoginEvent): Long = {
          element.eventTime * 1000L
        }
      }
    )
    //4 过滤出登录失败的，keyby（因为状态要基于keyby），处理
    //使用CEP来完成
//    这个只是用来定义规则
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    val keyedKS: KeyedStream[LoginEvent, Long] = timeDS.keyBy(_.userId)
//    CEP调用规则
    val ps: PatternStream[LoginEvent] = CEP.pattern(keyedKS,pattern)

    ps.select(map=>map.toString())
  }


  def lastMethod() = {
    //4 过滤出登录失败的，keyby（因为状态要基于keyby），处理
    //  timeDS.filter (_.eventType == "fail")
    //  .keyBy (_.userId)
    //  .process (
    //  new KeyedProcessFunction[Long, LoginEvent, String] {
    //  private var lastLoginEvent: ValueState[LoginEvent] = _
    //
    //  override def open (parameters: Configuration): Unit = {
    //  lastLoginEvent = getRuntimeContext.getState (
    //  new ValueStateDescriptor[LoginEvent] ("lastLoginEvent", classOf[LoginEvent] )
    //  )
    //}
    //
    //  override def processElement (value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, String] #Context, out: Collector[String] ): Unit = {
    //  val last: LoginEvent = lastLoginEvent.value ()
    //
    //  if (last != null && value.eventTime <= last.eventTime + 2) {
    //  out.collect (value.userId + "连续2s登录失败两次")
    //}
    //  lastLoginEvent.update (value)
    //}
    //
    //}
    //  )
  }
}

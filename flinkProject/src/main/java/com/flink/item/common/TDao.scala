package com.flink.item.common

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.flink.item.MarketingUserBehavior
import com.flink.item.util.FlinkStreamEnv
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, _}

import scala.util.Random

/**
  * 通用数据访问
  */
trait TDao {
  /**
    * 自定义数据源
    */
  def readMySource() = {
    FlinkStreamEnv.get().addSource(
      new RichParallelSourceFunction[MarketingUserBehavior]() {
        var running = true
        val channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")
        val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")
        val rand: Random = Random

        override def run(ctx: SourceContext[MarketingUserBehavior]): Unit = {
          val maxElements = Long.MaxValue
          var count = 0L

          while (running && count < maxElements) {
            val id = UUID.randomUUID().toString
            val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
            val channel = channelSet(rand.nextInt(channelSet.size))
            val ts = System.currentTimeMillis()

            ctx.collectWithTimestamp(MarketingUserBehavior(id, behaviorType, channel, ts), ts)
            count += 1
            TimeUnit.MILLISECONDS.sleep(5L)
          }
        }

        override def cancel(): Unit = running = false

      }
    )
  }

  /**
    * 从kafka读取数据
    *
    * @return
    */
  def readKafka() = {
    //    FlinkStreamEnv.get().addSource(
    //      new FlinkKafkaConsumer011[]()
    //    )
  }

  /**
    * 文本读取数据
    *
    * @param path
    * @return
    */
  def readTextFile(implicit path: String) = {
    FlinkStreamEnv.get().readTextFile(path)
  }

  /**
    * 从网络读取数据
    */
  def readSocket(): Unit = {

  }
}

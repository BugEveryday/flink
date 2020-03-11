package com.flink.item.service

import com.flink.item.{AdClickLog, CountByProvince}
import com.flink.item.common.{TDao, TService}
import com.flink.item.dao.AdClickDao
import com.flink.item.function.AdBlackListFunction
import com.flink.item.util.FlinkStreamEnv
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function. WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AdClickService extends TService {
  private val dao = new AdClickDao

  override def getDao(): TDao = dao

  override def analyses() = {
    //1 数据
    val dataDS: DataStream[String] = FlinkStreamEnv.get().readTextFile("input/AdClickLog.csv")

    //2 封装
    val logDS: DataStream[AdClickLog] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        AdClickLog(
          datas(0).toLong,
          datas(1).toLong,
          datas(2),
          datas(3),
          datas(4).toLong
        )
      }
    )
    // 3 时间戳和水位线
    val timeDS: DataStream[AdClickLog] = logDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[AdClickLog](Time.seconds(5)) {
        override def extractTimestamp(element: AdClickLog): Long = {
          element.timestamp * 1000L
        }
      }
    )
    //4 初步筛选。将一天内点击超过100次的加入黑名单
    val keyedDS: KeyedStream[(String, Long), String] = timeDS.map(log => (log.city + "_" + log.adId, 1L)).keyBy(_._1)

    val useDS: DataStream[(String, Long)] = keyedDS.process(new AdBlackListFunction)
    //这里是筛选出来的黑名单
    val outputTag = new OutputTag[(String, Long)]("blackList")
    useDS.getSideOutput(outputTag).print("blackList>>>")
    //    5 筛选后的数据

    val resultDS: DataStream[CountByProvince] = useDS.keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(
        new AggregateFunction[(String, Long), Long, Long] {
          override def add(value: (String, Long), accumulator: Long): Long = accumulator + value._2

          override def createAccumulator(): Long = 0L

          override def getResult(accumulator: Long): Long = accumulator

          override def merge(a: Long, b: Long): Long = a + b
        },
        new WindowFunction[Long, CountByProvince, String, TimeWindow] {
          override def apply(key: String, window: TimeWindow, elements: Iterable[Long], out: Collector[CountByProvince]): Unit = {
            val ks = key.split("_")
            val count = elements.iterator.next
            out.collect(
              CountByProvince(
                window.getEnd.toString,
                ks(0),
                ks(1).toLong,
                count
              )
            )
          }
        }
      )

    val resultKeyedDS: KeyedStream[CountByProvince, String] = resultDS.keyBy(_.windowEnd)

    //为了格式化输出
    resultKeyedDS.process(
      new KeyedProcessFunction[String, CountByProvince, String] {
        override def processElement(value: CountByProvince, ctx: KeyedProcessFunction[String, CountByProvince, String]#Context, out: Collector[String]): Unit = {
          out.collect("省份：" + value.province + ", 广告：" + value.adId + ",点击数量: " + value.count)
        }
      }
    )
  }
}

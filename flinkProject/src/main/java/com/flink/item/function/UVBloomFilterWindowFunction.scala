package com.flink.item.function

import java.lang
import java.sql.Timestamp

import com.flink.item.util.BloomFilter
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * 经过trigger，每条数据一条一条的进入这个方法
  */
class UVBloomFilterWindowFunction extends ProcessAllWindowFunction[(Long, Int), String, TimeWindow] {
  private var jedis: Jedis = _

  override def open(parameters: Configuration): Unit = {
    jedis = new Jedis("hadoop222",6379)
  }

  override def process(context: Context, elements: Iterable[(Long, Int)], out: Collector[String]): Unit = {
    //iter中的long是id，int是计数

    //取出id，转换为offset，查看在不在，不在就存，在就不管
    val userid: String = elements.iterator.next()._1.toString

    val offset: Long = BloomFilter.offset(userid, 5)

//    redis的位图需要key和value，key设置为endtime，方便最后汇总计数
    val key: String = context.window.getEnd.toString

    val flag: lang.Boolean = jedis.getbit(key,offset)
    if(!flag){//如果没有，就执行下面的操作
      //更新位图
      jedis.setbit(key,offset,true)
      // 更新UV数量
      val uv: String = jedis.hget("uvcount", key)
      var uvCount : Long = 0
      if ( uv != null && "" != uv ) {
        uvCount = uv.toLong
      }
      jedis.hset("uvcount", key, (uvCount + 1).toString)
      out.collect(new Timestamp(context.window.getEnd) + "新的独立访客 = " + userid)

    }


  }
}

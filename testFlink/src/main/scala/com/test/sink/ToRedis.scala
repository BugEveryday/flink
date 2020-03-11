package com.test.sink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object ToRedis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[String] = env.readTextFile("input")

    ds.print("toRedis==>")

    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop222").setPort(6379).build()

    ds.addSink(new RedisSink[String](conf,
      new RedisMapper[String] {
        override def getCommandDescription: RedisCommandDescription = {
          new RedisCommandDescription(RedisCommand.HSET, "flinkTest")
        }

        override def getValueFromData(t: String): String = {
          t.split(" ")(0)
        }

        override def getKeyFromData(t: String): String = {
          t.split(" ")(1)
        }
      }))

    env.execute()

  }

}

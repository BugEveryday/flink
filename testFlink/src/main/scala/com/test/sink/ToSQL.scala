package com.test.sink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}

object ToSQL {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[String] = env.fromCollection(List("1"))

    ds.addSink(new MyJdbcSink())

    env.execute()
  }

}

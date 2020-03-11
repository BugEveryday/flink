package com.test.trans

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}

object S09_Union {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5))
    val ds1: DataStream[Int] = ds.map((_ * 2))

    val unionDS: DataStream[Int] = ds.union(ds1)

    unionDS.print()

    env.execute()
  }

}

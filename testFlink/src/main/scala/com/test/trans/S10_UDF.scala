package com.test.trans

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object S10_UDF {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5))

    val myDS: DataStream[String] = ds.map(new S10_MyUDF)
    myDS.print()

    env.execute()
  }

}
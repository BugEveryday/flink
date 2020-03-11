package com.test.trans

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object S08_Connect {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5))

    val ds1: DataStream[Int] = ds.map((_ * 2))

    val conDS: ConnectedStreams[Int, Int] = ds.connect(ds1)

    val result: DataStream[String] = conDS.map(t1 => "ds" + t1, t2 => "ds1" + t2)
    result.print()


    env.execute()
  }

}

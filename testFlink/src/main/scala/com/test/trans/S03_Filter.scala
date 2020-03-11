package com.test.trans

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object S03_Filter {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[Int] = env.fromCollection(List(1,2,3,4,5))

    val filterDS: DataStream[Int] = ds.filter(_%2==0)

    filterDS.print()

    env.execute()
  }

}

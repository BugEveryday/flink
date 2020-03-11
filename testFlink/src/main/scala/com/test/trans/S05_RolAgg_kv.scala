package com.test.trans

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object S05_RolAgg_kv {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[(String, Int)] = env.fromCollection(List(("a", 1), ("b", 1), ("a", 2), ("b", 2)))

    val keyByDS: KeyedStream[(String, Int), Tuple] = ds.keyBy(0)
    keyByDS.print("keyBy")
//  滚动聚合算子：  针对KeyedStream的每一个支流做聚合。应该是进入一个，计算一个
    val sum: DataStream[(String, Int)] = keyByDS.sum(1)
    sum.print("sum")

    val min: DataStream[(String, Int)] = keyByDS.min(1)
    min.print("min")

    val max: DataStream[(String, Int)] = keyByDS.max(1)
    max.print("max")


    env.execute()
  }


}

package com.test.trans

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment,_}

object S06_Reduce_kv {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val ds: DataStream[(String, Int)] = env.fromCollection(List(("a", 1), ("b", 1), ("a", 2), ("b", 2)))

    val keyByDS: KeyedStream[(String, Int), Tuple] = ds.keyBy(0)
    keyByDS.print("keyBy")

    val reduceDS: DataStream[(String, Int)] = keyByDS.reduce((a,b)=>(a._1,a._2+b._2))

    reduceDS.print("reduce")

    env.execute()
  }

}

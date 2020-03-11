package com.test.trans

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object S04_KeyBy {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[(String, Int)] = env.fromCollection(List(("a", 1), ("b", 1), ("a", 2), ("b", 2)))

    val keyByDS_Scala: KeyedStream[(String, Int), Int] = ds.keyBy(_._2)

    keyByDS_Scala.print("scala")

    print("--------------------------")//虽然在这里，但是还是先输出的

    val keyByDS_java: KeyedStream[(String, Int), Tuple] = ds.keyBy(0)
    keyByDS_java.print("java")

    env.execute()
  }

}

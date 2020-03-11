package com.test.trans

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment,_}

object S07_SplitAndSelect {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[(String, Int)] = env.fromCollection(List(("a", 1), ("b", 1), ("a", 2), ("b", 2)))

    val keyByDS: KeyedStream[(String, Int), Tuple] = ds.keyBy(0)
    keyByDS.print("keyBy")

//    该方法已经过时，要使用side out
    val splitDS: SplitStream[(String, Int)] = keyByDS.split(t => {
      if (t._1 == "a") {
        Seq("bb", "BB")//就是给分组取个名，可以多个名字
      } else {
        Seq("aa", "AA")
      }
    })
    splitDS.print("split")

    val selectDS: DataStream[(String, Int)] = splitDS.select("aa")
    selectDS.print("select_aa")//select_aa:1> (b,1)   select_aa:1> (b,2)

    val selectDS2: DataStream[(String, Int)] = splitDS.select("BB")
    selectDS2.print("select_BB")//select_BB:3> (a,1)  select_BB:3> (a,2)



    env.execute()
  }

}

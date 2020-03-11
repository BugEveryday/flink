package com.test.trans

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}

object Rich {
/*
 * 所有Flink函数类都有其Rich版本
 * getRuntimeContext()方法提供了函数的RuntimeContext的一些信息
 */
def main(args: Array[String]): Unit = {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val ds: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5))

  val myDS: DataStream[String] = ds.map(new MyRichUDF)
  myDS.print()

  env.execute("rich")
}

}

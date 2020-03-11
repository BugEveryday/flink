package com.test.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._


object FromElements {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val javaBeanDS: DataStream[Person] = env.fromElements(new Person(1,1),new Person(2,2))

    javaBeanDS.print()

    env.execute()
  }

}

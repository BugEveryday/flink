package com.test.source


import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FromFile {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)//保证输出的有序

    val ds: DataStream[String] = env.readTextFile("input/wordcount.txt")

    ds.print()

    env.execute("textFile")

  }

}

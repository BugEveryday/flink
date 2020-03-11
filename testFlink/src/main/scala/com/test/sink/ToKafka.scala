package com.test.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object ToKafka {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[String] = env.readTextFile("input/wordcount.txt")

    ds.print("toKafka==>")

   ds.addSink(new FlinkKafkaProducer011[String]("hadoop222:9092","testFlink_sensor",new SimpleStringSchema()))

    env.execute("toKafka")


  }

}

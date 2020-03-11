package com.test.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object FromKafka {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "hadoop222:9092")
    prop.setProperty("group.id", "yang")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset", "latest")


    val kafkaDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("flinkTest_sensor", new SimpleStringSchema(), prop))
    print("kafka")
    kafkaDS.print("kafka=>")

    env.execute("flinkTest")

  }

}
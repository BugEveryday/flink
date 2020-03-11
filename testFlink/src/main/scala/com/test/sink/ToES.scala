package com.test.sink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object ToES {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds: DataStream[String] = env.readTextFile("input")

    ds.print("toELK==>")


    val hosts = new util.ArrayList[HttpHost]()
    hosts.add(new HttpHost("hadoop222",9200))

    val esBuilder = new ElasticsearchSink.Builder[String](hosts,
      new ElasticsearchSinkFunction[String] {
        override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          print("保存" + t)
          val strings: Array[String] = t.split(" ")
          val map = new util.HashMap[String,String]()
          map.put(strings(0),strings(1))
          //source必须是even偶数的
          //index的名字必须是lowcase小写
          val indexRequest: IndexRequest = Requests.indexRequest().index("flinktest").`type`("_doc").source(map)
          requestIndexer.add(indexRequest)
          print("success")
        }
      })
    
    ds.addSink(esBuilder.build())


    env.execute()

  }

}

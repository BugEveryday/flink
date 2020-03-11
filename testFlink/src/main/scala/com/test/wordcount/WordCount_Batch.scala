package com.test.wordcount

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, _}

object WordCount_Batch {
  def main(args: Array[String]): Unit = {

//    创建Flink的上下文执行环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
//  因为是流，所以没有结束，只有有界流，比如读取个文件
    //获取数据
    //开发环境中，相对路径是从idea的主项目中查找
    val ds: DataSet[String] = environment.readTextFile("input/wordcount.txt")

    val wc: AggregateDataSet[(String, Int)] = ds.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    wc.print()



  }

}

package com.test.trans

import org.apache.flink.api.common.functions.RichMapFunction

class MyRichUDF extends RichMapFunction[Int,String]{
  override def map(in: Int): String = {
    val name: String = getRuntimeContext.getTaskName
    name+"->"+in*2+",i am rich"
  }
}

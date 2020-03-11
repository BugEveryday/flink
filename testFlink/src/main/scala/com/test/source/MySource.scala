package com.test.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

class MySource extends SourceFunction[String]{
  var flag = true
  override def cancel(): Unit = {
    flag = false
  }

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    var i = 0
    while (flag){
      sourceContext.collect((i+1).toString)
      i = i+1
      Thread.sleep(1000)
    }
  }
}

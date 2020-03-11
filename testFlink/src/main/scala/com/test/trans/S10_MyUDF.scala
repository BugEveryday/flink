package com.test.trans

import org.apache.flink.api.common.functions.MapFunction

// 1. 继承（实现）MapFunction接口，并定义泛型（输入，输出）
// 2. 实现方法
class S10_MyUDF extends MapFunction[Int, String] {
  override def map(t: Int): String = {
    //其实底层还是用了map
    "prefix"+ t*2
  }
}

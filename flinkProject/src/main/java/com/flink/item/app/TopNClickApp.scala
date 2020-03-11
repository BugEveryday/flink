package com.flink.item.app

import com.flink.item.common.TApp
import com.flink.item.controller.TopNClickController

object TopNClickApp extends App with TApp {
//  启动应用程序
  start{//里面都是要传入的函数--op
//    启动控制器
    val controller = new TopNClickController
    controller.execute()
  }

}

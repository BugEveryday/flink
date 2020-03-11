package com.flink.item.app

import com.flink.item.common.TApp
import com.flink.item.controller.TopNUrlController

object TopNUrlApp extends App with TApp{
  start{
    val controller = new TopNUrlController
    controller.execute()
  }

}

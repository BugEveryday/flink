package com.flink.item.app

import com.flink.item.common.TApp
import com.flink.item.controller.UVController

object UVApp extends App with TApp{
  start{
    val controller = new UVController
    controller.execute()
  }

}

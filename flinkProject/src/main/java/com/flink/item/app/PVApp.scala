package com.flink.item.app

import com.flink.item.common.TApp
import com.flink.item.controller.PVController

object PVApp extends App with TApp{
  start{
    val controller = new PVController
    controller.execute()
  }

}

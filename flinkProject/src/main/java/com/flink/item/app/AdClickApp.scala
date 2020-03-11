package com.flink.item.app

import com.flink.item.common.TApp
import com.flink.item.controller.AdClickController

object AdClickApp extends App with TApp{
  start{
    val controller = new AdClickController
    controller.execute()
  }

}

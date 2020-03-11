package com.flink.item.app

import com.flink.item.common.TApp
import com.flink.item.controller.AppMarketController

object AppMarketApp extends App with TApp{
  start{
    val controller = new AppMarketController
    controller.execute()
  }

}

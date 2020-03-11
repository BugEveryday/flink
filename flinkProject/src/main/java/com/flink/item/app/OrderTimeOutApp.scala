package com.flink.item.app

import com.flink.item.common.TApp
import com.flink.item.controller.OrderTimeOutController

object OrderTimeOutApp extends App with TApp{
start{
  val controller = new OrderTimeOutController
  controller.execute()
}
}

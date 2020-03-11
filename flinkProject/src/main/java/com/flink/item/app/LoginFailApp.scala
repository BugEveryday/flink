package com.flink.item.app

import com.flink.item.common.TApp
import com.flink.item.controller.LoginFailController

object LoginFailApp extends App with TApp{
  start{
    val controller = new LoginFailController
    controller.execute()
  }

}

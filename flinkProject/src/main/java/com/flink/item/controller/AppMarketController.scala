package com.flink.item.controller

import com.flink.item.common.TController
import com.flink.item.service.AppMarketService
import org.apache.flink.streaming.api.scala.DataStream

class AppMarketController extends TController {
  private val service = new AppMarketService

  override def execute() = {
 val result: DataStream[String] = service.analyses()
    result.print()

  }
}

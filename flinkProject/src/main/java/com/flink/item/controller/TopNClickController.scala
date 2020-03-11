package com.flink.item.controller

import com.flink.item.common.TController
import com.flink.item.service.TopNClickService

class TopNClickController extends TController {

  private val service = new TopNClickService

  override def execute(): Unit = {
    val result = service.analyses()
    result.print()
  }
}

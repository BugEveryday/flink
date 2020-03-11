package com.flink.item.controller

import com.flink.item.common.TController
import com.flink.item.service.TopNUrlService

class TopNUrlController extends TController {
  private val service = new TopNUrlService

  override def execute(): Unit = {
    val result = service.analyses()
    result.print()
  }
}

package com.flink.item.controller

import com.flink.item.common.TController
import com.flink.item.service.AdClickService

class AdClickController extends TController{
  private val service = new AdClickService
  override def execute(): Unit = {
    service.analyses().print()
  }

}

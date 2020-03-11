package com.flink.item.controller

import com.flink.item.common.TController
import com.flink.item.service.PVService

class PVController extends TController{
  private val service = new PVService
  override def execute(): Unit = {
    val result = service.analyses()
    result.print()
  }
}

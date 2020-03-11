package com.flink.item.controller

import com.flink.item.common.TController
import com.flink.item.service.OrderTimeOutService

class OrderTimeOutController extends TController{
  private val service = new OrderTimeOutService
  override def execute(): Unit = {
    service.analyses().print()
  }
}

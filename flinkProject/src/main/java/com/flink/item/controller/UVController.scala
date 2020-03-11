package com.flink.item.controller

import com.flink.item.common.TController
import com.flink.item.service.UVService
import org.apache.flink.streaming.api.scala.DataStream

class UVController extends TController {
  private val service = new UVService

  override def execute(): Unit = {
    val result: DataStream[String] = service.analyses()
    result.print()
  }
}

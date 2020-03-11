package com.flink.item.controller

import com.flink.item.common.TController
import com.flink.item.service.LoginFailService
import org.apache.flink.streaming.api.scala.DataStream

class LoginFailController extends TController {
  private val service = new LoginFailService

  override def execute(): Unit = {
    val result: DataStream[String] = service.analyses()
    result.print()

  }
}

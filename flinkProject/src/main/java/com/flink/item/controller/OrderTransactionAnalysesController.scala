package com.flink.item.controller

import com.flink.item.common.TController
import com.flink.item.service.OrderTransactionAnalysesService


class OrderTransactionAnalysesController extends TController{

    private val orderTransactionAnalysesService = new OrderTransactionAnalysesService

    override def execute(): Unit = {
        val result = orderTransactionAnalysesService.analyses()
        result.print
    }
}

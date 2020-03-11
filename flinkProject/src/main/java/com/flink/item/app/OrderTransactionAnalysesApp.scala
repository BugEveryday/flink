package com.flink.item.app

import com.flink.item.common.TApp
import com.flink.item.controller.OrderTransactionAnalysesController


object OrderTransactionAnalysesApp extends App with TApp {

    start {
        val controller = new OrderTransactionAnalysesController
        controller.execute()
    }
}

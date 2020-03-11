package com.flink.item.function

import com.flink.item.ApacheLog
import org.apache.flink.api.common.functions.AggregateFunction

class TopNUrlAggregateFunction extends AggregateFunction[ApacheLog, Long, Long] {
  override def add(value: ApacheLog, accumulator: Long): Long = accumulator + 1L

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

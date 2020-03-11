package com.flink.item.function

import com.flink.item.TopNUrlClick
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class TopNUrlWindowFunction extends WindowFunction[Long,TopNUrlClick,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[TopNUrlClick]): Unit = {
    out.collect(TopNUrlClick(key,input.iterator.next(),window.getEnd))
  }
}

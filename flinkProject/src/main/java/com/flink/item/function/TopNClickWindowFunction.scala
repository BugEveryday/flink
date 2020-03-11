package com.flink.item.function

import com.flink.item.bean.TopNClick
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class TopNClickWindowFunction extends WindowFunction[Long,TopNClick,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[TopNClick]): Unit = {
    out.collect(TopNClick(key,input.iterator.next(),window.getEnd))
  }
}

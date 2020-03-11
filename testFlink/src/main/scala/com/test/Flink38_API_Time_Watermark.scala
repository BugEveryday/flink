package com.test

import java.text.SimpleDateFormat

import com.test.source.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink38_API_Time_Watermark {

    def main(args: Array[String]): Unit = {

        // TODO  API - Window
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // Watermark : 水位线（水印）
        // 0 - 3
        // 其实是对迟到数据处理的机制，但是watermark达到数据窗口的结束的时候，触发窗口计算
        // 当窗口时间到达时，本身应该触发计算，但是为了能够对迟到数据进行正确的处理
        // 需要将计算的时间点推迟，推迟到watermark标记到达时。

        //val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
        val dataDS: DataStream[String] = env.socketTextStream("hadoop222",9999)
        val sensorDS: DataStream[WaterSensor] = dataDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

        // 抽取时间戳和设定水位线（标记）
        // 1. 从数据中抽取数据作为事件时间
        // 2. 设定水位线标记，这个标记一般比当前数据事件时间要推迟

        // 1549044122000 => wm: 1549044125000
        val markDS: DataStream[WaterSensor] = sensorDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
                // 抽取事件时间,以毫秒为单位
                override def extractTimestamp(element: WaterSensor): Long = {
                    element.ts * 1000L
                }
            }
        )

        // 1. 时间窗口如何划分？
        //    timestamp - (timestamp - offset(0) + windowSize) % windowSize;
        //    1Min => 5 => 12段 (前闭后开)
        //    [00:00 - 00:05)
        //    [00:05 - 00:10)
        //    [00:10 - 00:15)
        //    [00:15 - 00:20)
        //    [00:20 - 00:25)
        //    [00:25 - 00:30)
        // 2. 标记何时触发窗口计算
        //    当标记(事件时间+推迟时间)大于窗口结束的时候就会触发窗口的计算
        //   5+3 => 8s
        //   10+3 => 13s

        val applyDS: DataStream[String] = markDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(5))
                .apply(
                    // 对窗口进行数据处理
                    // key : 分流的key
                    // window : 当前使用窗口的类型
                    // datas : 窗口中的数据
                    // out : 输出,指定输出的类型
                    (key: String, window: TimeWindow, datas: Iterable[WaterSensor], out: Collector[String]) => {
                        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                        val start = window.getStart
                        val end = window.getEnd
                        out.collect(s"[${start}-${end}), 数据[${datas}]")
                    }
                )


        markDS.print("mark>>>")
        applyDS.print("window>>>")


        env.execute()
    }

}

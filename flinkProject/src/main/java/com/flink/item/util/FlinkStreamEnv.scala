package com.flink.item.util

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//object 就可以直接用 类.方法 的形式调用了
object FlinkStreamEnv {
//  ThreadLocal工具类来操作线程中的共享变量
  private val envThreadLocal = new ThreadLocal[StreamExecutionEnvironment]
//  初始化
  def init() = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setStateBackend()
    envThreadLocal.set(env)
    env
  }
//  获取环境
  def get(): StreamExecutionEnvironment ={
    var env: StreamExecutionEnvironment = envThreadLocal.get()
    if(env==null){
      env = init()
    }
    env
  }
//  执行
  def executor() ={
    get().execute()
  }
//  清除
  def clear() ={
    envThreadLocal.remove()
  }
}

package com.flink.item.common

import com.flink.item.util.FlinkStreamEnv


trait TApp {
//  准备环境
  def start( op: =>Unit ): Unit = {
    try{
      //直接调用工具类创建环境，不用每次都创建了
      FlinkStreamEnv.init()
      //将逻辑（函数）作为参数传递-->控制抽象
      //业务操作的函数
      op
      FlinkStreamEnv.executor()
    }catch{
      case e:Exception=>e.printStackTrace()
    }finally {
      FlinkStreamEnv.clear()
    }
  }

}

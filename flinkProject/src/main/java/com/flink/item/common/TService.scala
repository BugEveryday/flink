package com.flink.item.common

import com.flink.item.bean.UserBehavior
import org.apache.flink.streaming.api.scala.{DataStream,_}

/**
  * 通用服务
  */
trait TService {
//  得到Dao
  def getDao():TDao
  //  分析方法
  def analyses(): Any

  //  获取用户行为数据，并进行封装
  def getUserBehaviorDatas() = {
    val userBehaviorDS: DataStream[String] = getDao().readTextFile("input/UserBehavior.csv")

    userBehaviorDS.map(
      line=>{
        val datas: Array[String] = line.split(",")
        UserBehavior(
          datas(0).toLong,
          datas(1).toLong,
          datas(2).toLong,
          datas(3),
          datas(4).toLong
        )
      }
    )
//    直接返回就行，不用非要给命名
  }
}

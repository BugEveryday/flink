package com.test.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MyJdbcSink() extends RichSinkFunction[String] {
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  //var updateStmt: PreparedStatement = _

  // open 主要是创建连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    conn = DriverManager.getConnection("jdbc:mysql://hadoop222:3306/test", "root", "000000")
    insertStmt = conn.prepareStatement("INSERT INTO base_region (id, region_name) VALUES (?, ?)")
    //updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
  }

  // 调用连接，执行sql
  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {

    insertStmt.setInt(1, 1)
    insertStmt.setString(2, "zzz")
    insertStmt.execute()
  }

  override def close(): Unit = {
    insertStmt.close()
    conn.close()
  }
}

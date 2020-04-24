package com.scz.flink.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.scz.flink.stream.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
  * Created by shen on 2019/12/11.
  */
object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.readTextFile("data/flink/sensor.txt")
    val dataStream = inputStream.map(data => {
      val split = data.split(",")
      SensorReading(split(0).trim, split(1).trim.toLong, split(2).trim.toDouble)
    })
    // sink
    dataStream.addSink(new MyJdbcSink())

    env.execute("jdbc sink...")
  }
}

class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  // 定义sql连接、预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  // 初始化，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?,?)")
    updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
  }

  // 调用连接，执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // 执行更新语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    // 如果update没有查到数据，那么执行插入语句
    if( updateStmt.getUpdateCount == 0 ){
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  // 关闭时做清理工作
  override def close(): Unit = {
    if (insertStmt != null) {
      insertStmt.close()
    }
    if (updateStmt != null) {
      updateStmt.close()
    }
    if (conn != null) {
      conn.close()
    }
  }
}
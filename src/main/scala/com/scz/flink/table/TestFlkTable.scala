package com.scz.flink.table

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

/**
  * Created by shen on 2019/12/23.
  */
object TestFlkTable {
  def main(args: Array[String]): Unit = {
    testScan()
  }

  def testScan(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )
    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)
    //注册table
    tableEnv.registerTable("user1",table)
    //查询table 所有数据
    tableEnv.scan("user1")
      //重命令字段名称
      .as('id,'name,'value)
      //选择需要的字段
      .select('id,'name,'value)
      //条件过滤
      .where("value=20")
      .first(1)
      .print()
  }

}

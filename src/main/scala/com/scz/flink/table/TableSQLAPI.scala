package com.scz.flink.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

/**
  * Created by shen on 2019/12/23.
  */
object TableSQLAPI {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val csv = env.readCsvFile[SalesLog]("data/flink/sales.csv", ignoreFirstLine=true)
//    csv.print()
//    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    // DataSet ==> Table
    val salesTable = tableEnv.fromDataSet(csv)
    // Table ==> table
    tableEnv.registerTable("sales", salesTable)
    // sql
    val resultTable = tableEnv.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId")
    tableEnv.toDataSet[Row](resultTable).print()
  }
}

case class SalesLog(transactionId:String, customerId:String, itemId:String, amountPaid:Double)

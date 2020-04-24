package com.scz.flink.core

import org.apache.flink.api.scala._

/**
  * Created by shen on 2019/12/22.
  */
object TestSource {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    fromCollectionDemo(env)
//    readCsvFileDemo(env);
//    readCompressionFiles(env)
  }

  def fromCollectionDemo(env : ExecutionEnvironment): Unit = {
    val collectionDS = env.fromCollection(List(
      ("Tom", 23), ("Jack", 25),
      ("Smith", 28), ("Alan", 18), ("Tom", 2)
    ))
    collectionDS.groupBy(0).sum(1).print()
//    collectionDS.print()
  }

  def readCsvFileDemo(env : ExecutionEnvironment): Unit = {
//    val csvDS= env.readCsvFile[(Long, String, Long)]("data/flink/OrderLog.csv", ignoreFirstLine = false, includedFields = Array(0, 1, 3))
    val csvDS = env.readCsvFile[Order]("data/flink/OrderLog.csv")
    csvDS.print()
  }

  def readCompressionFiles(env : ExecutionEnvironment): Unit = {
    env.readTextFile("data/flink/seq").print()
  }

}

case class Order(orderId: Long, eventType: String, txId: String, eventTime: Long)

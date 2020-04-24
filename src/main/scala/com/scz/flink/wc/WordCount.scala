package com.scz.flink.wc

import java.util.StringTokenizer

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

/**
  * 批处理代码
  * Created by shen on 2019/12/10.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputDataSet = env.readTextFile("data/wc.txt");
    val wordCountDataSet = inputDataSet.flatMap(line => {
      println(Thread.currentThread().getId + " ==> " + line)
      val list = new scala.collection.mutable.ListBuffer[String]()
      val tokenizer = new StringTokenizer(line)
      while(tokenizer.hasMoreTokens()) {
        list.append(tokenizer.nextToken())
      }
      list
    }).map((_, 1)).groupBy(0).sum(1).setParallelism(1).sortPartition(1, Order.DESCENDING)
    wordCountDataSet.print()
  }
}

package com.scz.flink.core

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * Created by shen on 2019/12/22.
  *
  * 基于Flink编程的计数器开发三步曲
  * step1：定义计数器
  * step2: 注册计数器
  * step3: 获取计数器
  */
object CounterApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements("hadoop","spark","flink","pyspark","storm")
    val info = data.map(new RichMapFunction[String,String]() {
      // step1：定义计数器
      val counter = new LongCounter()
      override def open(parameters: Configuration): Unit = {
        // step2: 注册计数器
        getRuntimeContext.addAccumulator("ele-counts-scala", counter)
      }
      override def map(in: String): String = {
        counter.add(1)
        in
      }
    })
//    info.print()
    val outPath = "F:/index/result/acc"
    info.setParallelism(1).writeAsText(outPath, WriteMode.OVERWRITE)
    val jobResult = env.execute("CounterApp")
    // step3: 获取计数器
    val num = jobResult.getAccumulatorResult[Long]("ele-counts-scala")
    println("num: " + num)

  }
}

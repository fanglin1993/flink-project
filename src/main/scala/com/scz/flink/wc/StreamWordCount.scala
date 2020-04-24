package com.scz.flink.wc

import java.util.StringTokenizer

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by shen on 2019/12/10.
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
//    val params = ParameterTool.fromArgs(args)
//    val host: String = params.get("host")
//    val port: Int = params.getInt("port")

    // 创建一个流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(1)
    //    env.disableOperatorChaining()

    val host = "localhost"
    val port = 9999
    // 接收socket数据流
    val textDataStream = env.socketTextStream(host, port)

    // 逐一读取数据，分词之后进行wordcount
    val wordCountDataStream = textDataStream.flatMap(line => {
      val list = new scala.collection.mutable.ListBuffer[String]()
      val tokenizer = new StringTokenizer(line)
      while(tokenizer.hasMoreTokens()) {
        list.append(tokenizer.nextToken())
      }
      list
    }).filter(_.nonEmpty).startNewChain()
      .map( (_, 1) )
      .keyBy(0)
      .timeWindow(Time.seconds(10))
      .sum(1)

    // 打印输出
    wordCountDataStream.print().setParallelism(1)

    // 执行任务
    env.execute("stream word count job")

  }
}

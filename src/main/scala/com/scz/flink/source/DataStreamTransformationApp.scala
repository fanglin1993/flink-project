package com.scz.flink.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * Created by shen on 2019/12/22.
  */
object DataStreamTransformationApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    nonParallelSourceFunction(env)
    parallelSourceFunction(env)
    env.execute("source test")
  }

  def nonParallelSourceFunction(env : StreamExecutionEnvironment): Unit = {
    val nonParallelSource : DataStream[Long] = env.addSource(new CustomNonParallelSourceFunction)
    nonParallelSource.print().setParallelism(1)
  }

  def parallelSourceFunction(env : StreamExecutionEnvironment): Unit = {
    val nonParallelSource : DataStream[Long] = env.addSource(new CustomParallelSourceFunction)
    nonParallelSource.print().setParallelism(2)
  }

}

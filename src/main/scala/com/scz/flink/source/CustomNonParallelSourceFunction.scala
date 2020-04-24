package com.scz.flink.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * Created by shen on 2019/12/22.
  */
class CustomNonParallelSourceFunction extends SourceFunction[Long] {

  var count = 1L
  var isRunning = true

  override def cancel(): Unit = {
    isRunning = false
  }

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning && count < 1000) {
      ctx.collect(count)
      count += 1
      Thread.sleep(500)
    }
  }
}

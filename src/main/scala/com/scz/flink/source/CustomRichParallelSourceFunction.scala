package com.scz.flink.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
  * Created by shen on 2019/12/22.
  */
class CustomRichParallelSourceFunction extends RichParallelSourceFunction[Long] {
  var count = 1L
  var isRunning = true

  override def cancel(): Unit = {
    isRunning = false
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning && count < 1000) {
      ctx.collect(count)
      count += 1
      Thread.sleep(500)
    }
  }
}

package com.scz.flink.stream

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by shen on 2019/12/12.
  */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500)

    val inputStream = env.readTextFile("data/flink/sensor.txt")
    val dataStream = inputStream.map(data => {
      val split = data.split(",")
      SensorReading(split(0).trim, split(1).trim.toLong, split(2).trim.toDouble)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    val minTempWindowStream = dataStream.map(data => (data.id, (data.temperature, 1)))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10)) // 开时间窗口
      .reduce((data1, data2) => (data1._1, (data1._2._1 + data2._2._1, data1._2._2 + data2._2._2))) // 用reduce做增量聚合
      .map(data => (data._1, data._2._1 / data._2._2))
    minTempWindowStream.print("input data")

    env.execute("window start...")
  }
}

class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{
  // 定义固定延迟为3秒
  val bound: Long = 3 * 1000L
  // 定义当前收到的最大的时间戳
  var maxTs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000L)
    element.timestamp * 1000L
  }
}

class MyAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading]{
  val bound: Long = 1000L

  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if( lastElement.id == "sensor_1" ){
      new Watermark(extractedTimestamp - bound)
    }else{
      null
    }
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000L
  }
}

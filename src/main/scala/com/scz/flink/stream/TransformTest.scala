package com.scz.flink.stream

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Created by shen on 2019/12/11.
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // 读入数据
    val inputStream : DataStream[String] = env.readTextFile("data/flink/sensor.txt")

    val dataStream = inputStream.map(data => {
      val split = data.split(",")
      SensorReading(split(0).trim, split(1).trim.toLong, split(2).trim.toDouble)
    })

    val stream1 = dataStream.keyBy("id")
//      .sum("temperature")
      .reduce((x, y) => {
      println("==> " + x)
      println(">>> " + y)
      SensorReading(x.id, x.timestamp + 1, y.temperature + 10)
    })

    // 2. 分流，根据温度是否大于30度划分
    val splitStream = dataStream.split(sensorData => {
        if( sensorData.temperature > 30 ) Seq("high") else Seq("low")
      })

    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high", "low")

    // 3. 合并两条流
    val warningStream = highTempStream.map( sensorData => (sensorData.id, sensorData.temperature) )
    val connectedStreams = warningStream.connect(lowTempStream)

    val coMapStream = connectedStreams.map(
      warningData => ( warningData._1, warningData._2, "high temperature warning" ),
      lowData => ( lowData.id, "healthy" )
    )

    val unionStream = highTempStream.union(lowTempStream)

    coMapStream.print("coMapStream")

    env.execute("transform test start...")

  }
}

class MyFlatMap extends RichFlatMapFunction[Int, (Int, Int)] {

  var subTaskIndex = 0
  override def open(configuration: Configuration): Unit = {
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
    // 以下可以做一些初始化工作，例如建立一个和HDFS的连接
  }

  override def flatMap(in: Int, out: Collector[(Int, Int)]): Unit = {
    if (in % 2 == subTaskIndex) {
      out.collect((subTaskIndex, in))
    }
  }

  override def close(): Unit = {
    // 以下做一些清理工作，例如断开和HDFS的连接。
  }

}


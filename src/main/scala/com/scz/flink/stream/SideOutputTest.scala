package com.scz.flink.stream

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Created by shen on 2019/12/12.
  */
object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    val inputStream = env.socketTextStream("localhost", 7777)
    val dataStream = env.addSource(new SensorSource)
    .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading]( Time.seconds(1) ) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
    } )

    val processedStream = dataStream
      .process(new FreezingAlert())

    processedStream.print("processed data")
    processedStream.getSideOutput( new OutputTag[String]("freezing alert") ).print("alert data")

    env.execute("side output test")

  }
}

// 冰点报警，如果小于32F，输出报警信息到侧输出流
class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading]{

  //  lazy val alertOutput: OutputTag[String] = new OutputTag[String]( "freezing alert" )

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if( value.temperature < 32.0 ){
      ctx.output( new OutputTag[String]( "freezing alert" ), "freezing alert for " + value.id )
    }
    out.collect( value )
  }
}
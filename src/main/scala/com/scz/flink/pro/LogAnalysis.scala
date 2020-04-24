package com.scz.flink.pro

import java.util.Date
import java.util.Properties

import com.scz.flk.util.DateUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

/**
  * Created by shen on 2019/12/24.
  */
object LogAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    import org.apache.flink.api.scala._
    val topic = "test"

    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "test-group")

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), props)

    // 接收Kafka数据
    val data : DataStream[String] = env.addSource(consumer)

    val logData = data.map(x => {
      val splits = x.split("\t")
      val level = splits(2)

      var time = DateUtils.parse(splits(3))

      val domain = splits(5)
      val traffic = splits(6).toLong

      (level, time, domain, traffic)
    }).filter(_._2 != 0).filter(_._1 == "E")
      .map(x => {
        (x._2, x._3, x._4)   // 1 level(抛弃)  2 time  3 domain   4 traffic
      })

    val resultData = logData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long,String,Long)] {

      val maxOutOfOrderness = 3500L  // 3.5 seconds
      var currentMaxTimestamp: Long = _

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1) //此处是按照域名进行keyBy的
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply(new WindowFunction[(Long,String,Long),(String,String,Long),Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {

          val domain = key.getField(0).toString
          var sum = 0l

          val times = ArrayBuffer[Long]()

          val iterator = input.iterator
          while(iterator.hasNext) {
            val next = iterator.next()
            sum += next._3  // traffic求和
            // TODO... 是能拿到你这个window里面的时间的  next._1
            times.append(next._1)
          }

          /**
            *  第一个参数：这一分钟的时间 2019-09-09 20:20
            *  第二个参数：域名
            *  第三个参数：traffic的和
            */
          val time = DateUtils.format(new Date(times.max))
          out.collect((time,domain,sum))
        }
      })

    env.execute("LogAnalysis")
  }
}

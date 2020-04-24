package com.scz.flink.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

/**
  * Created by shen on 2019/12/23.
  */
object KafkaConnectorProducerApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 从socket接收数据，通过Flink，将数据Sink到Kafka
    val data = env.socketTextStream("sparktest", 9999)
    val topic = "test"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "sparktest:9092")

    val kafkaSink = new FlinkKafkaProducer[String](topic,
          new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
          properties)

//    val kafkaSink = new FlinkKafkaProducer[String](topic,
//      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
//      properties,
//      FlinkKafkaProducer.Semantic.EXACTLY_ONCE)

    data.addSink(kafkaSink)
    env.execute("KafkaConnectorProducerApp")
  }
}

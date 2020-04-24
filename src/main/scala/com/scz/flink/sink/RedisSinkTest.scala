package com.scz.flink.sink

import com.scz.flink.stream.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * Created by shen on 2019/12/11.
  */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // source
    val inputStream = env.readTextFile("data/flink/sensor.txt")

    // transform
    val dataStream = inputStream.map(data => {
      val split = data.split(",")
      SensorReading(split(0).trim, split(1).trim.toLong, split(2).trim.toDouble)
    })

    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
//      .setPassword("123456")
      .setPort(6379)
      .build()

    // sink
    dataStream.addSink(new RedisSink(conf, new MyRedisMapper()))

    env.execute("redis sink...")
  }
}

class MyRedisMapper() extends RedisMapper[SensorReading]{

  // 定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    // 把传感器id和温度值保存成哈希表 HSET key field value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  // 定义保存到redis的value
  override def getValueFromData(t: SensorReading): String = t.temperature.toString

  // 定义保存到redis的key
  override def getKeyFromData(t: SensorReading): String = t.id
}

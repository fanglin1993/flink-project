package com.scz.flk.source;

import com.scz.flk.model.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

/**
 * Created by shen on 2019/12/25.
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        fromCollection(env);
//        readTextFile(env);
//        fromKafka(env);
        costomSource(env);

        env.execute("SourceTest");
    }

    public static void fromCollection(StreamExecutionEnvironment env) {
        DataStream<SensorReading> stream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199, 35.80018327300259),
                new SensorReading("sensor_6", 1547718201, 15.402984393403084),
                new SensorReading("sensor_7", 1547718202, 6.720945201171228),
                new SensorReading("sensor_10", 1547718205, 38.101067604893444)
        ));
        stream.print().setParallelism(1);
    }

    public static void readTextFile(StreamExecutionEnvironment env) {
        DataStreamSource<String> stream = env.readTextFile("data/flink/sensor.txt");
        stream.print().setParallelism(1);
    }

    public static void fromKafka(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test-group");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), props));
        stream.print().setParallelism(1);
    }

    public static void costomSource(StreamExecutionEnvironment env) {
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource(500));
        SplitStream<SensorReading> splitStream = stream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                if (value.getTemperature() > 60) {
                    return Arrays.asList("high");
                }
                return Arrays.asList("low");
            }
        });
        DataStream<SensorReading> lowTempStream = splitStream.select("low");
        lowTempStream.print().setParallelism(1);
    }
}

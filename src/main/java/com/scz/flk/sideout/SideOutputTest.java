package com.scz.flk.sideout;

import com.scz.flk.model.SensorReading;
import com.scz.flk.source.SensorSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Created by shen on 2019/12/26.
 */
public class SideOutputTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource(100, true));
        SingleOutputStreamOperator<SensorReading> dataStream = stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp();
            }
        });
        final OutputTag<String> alertOutput = new OutputTag<String>("freezing alert") {};
        SingleOutputStreamOperator<SensorReading> processedStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            // 冰点报警，如果小于32F，输出报警信息到侧输出流
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                if (value.getTemperature() < 42.0) {
                    ctx.output(alertOutput, "freezing alert for " + value.getId());
                } else {
                    out.collect(value);
                }
            }
        });

        processedStream.print("processed data");
        DataStream<String> sideOutputStream = processedStream.getSideOutput(alertOutput);
        sideOutputStream.print("alert data");

        env.execute("WindowTest");
    }
}

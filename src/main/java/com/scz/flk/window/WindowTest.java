package com.scz.flk.window;

import com.scz.flk.model.SensorReading;
import com.scz.flk.source.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * Created by shen on 2019/12/25.
 */
public class WindowTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        DataStreamSource<String> stream = env.readTextFile("data/flink/sensor.txt");
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource(100));
        SingleOutputStreamOperator<SensorReading> dataStream = stream.assignTimestampsAndWatermarks(new SensorTimeAssigner());
        SingleOutputStreamOperator<Tuple2<String, Double>> minTempWindowStream = dataStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        }).keyBy(0)
        .timeWindow(Time.seconds(10), Time.seconds(5))
        .reduce(new ReduceFunction<Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> reduce(Tuple2<String, Double> v1, Tuple2<String, Double> v2) throws Exception {
                return new Tuple2<>(v1.f0, Math.min(v1.f1, v2.f1));
            }
        });
        stream.print("input data");
        minTempWindowStream.print("min temp");

        env.execute("WindowTest");
    }

    /**
     * 周期性的生成watermark：系统会周期性的将watermark插入到流中(水位线也是一种特殊的事件!)。默认周期是200毫秒。可以使用ExecutionConfig.setAutoWatermarkInterval()方法进行设置。
     * 产生watermark的逻辑：每隔5秒钟，Flink会调用AssignerWithPeriodicWatermarks的getCurrentWatermark()方法，如果方法返回一个时间戳大于之前水位的时间戳，新的watermark会被插入到流中。这个检查保证了水位线是单调递增的。如果方法返回时间戳小于等之前水位的时间戳，则不会产生新的watermark。
     */
    static class PeriodicAssigner implements AssignerWithPeriodicWatermarks<SensorReading> {

        private long bound = 1000L;  // 延时1s
        private long maxTs = Long.MIN_VALUE;    // 观察到的最大时间戳

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTs - bound);
        }

        @Override
        public long extractTimestamp(SensorReading element, long previousElementTimestamp) {
            maxTs = Math.max(maxTs, element.getTimestamp());
            return element.getTimestamp();
        }
    }

    /**
     * 对于乱序数据流，如果我们能大致估算出数据流中的事件最延迟时间，就可以使用如下代码
     */
    static class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<SensorReading> {

        public SensorTimeAssigner() {
            super(Time.seconds(1));
        }

        @Override
        public long extractTimestamp(SensorReading element) {
            return element.getTimestamp();
        }
    }

    /**
     * 间断式地生成watermark
     * 和周期性生成的方式不同，这种方式不是固定时间的，而是可以根据需要对每条数进行筛选和处理。
     * 举个例子，我们只给sensor_1的传感器数据流插入watermark：
     */
    static class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<SensorReading> {

        private long bound = 1000L;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(SensorReading lastElement, long extractedTimestamp) {
            if ("sensor_1".equals(lastElement.getId())) {
                return new Watermark(lastElement.getTimestamp() - bound);
            }
            return null;
        }

        @Override
        public long extractTimestamp(SensorReading element, long previousElementTimestamp) {
            return element.getTimestamp();
        }
    }

}

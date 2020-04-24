package com.scz.flk.process;

import com.scz.flk.model.SensorReading;
import com.scz.flk.source.SensorSource;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Created by shen on 2019/12/26.
 */
public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(100000);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);  // CK失败任务也失败，默认是true
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);   // 同时CK的个数
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);   // 两个CK的间隔
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION); // 任务失败(取消)，是否处理CK

        env.setStateBackend(new FsStateBackend("file:///E:/IdeaSparkWS/flink-project/checkpoints"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));   // 重启3次，每次间隔5s


        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource(100, true));
        SingleOutputStreamOperator<SensorReading> dataStream = stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp();
            }
        });
        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading value) throws Exception {
                return value.getId();
            }
        });

//        SingleOutputStreamOperator<String> processedStream = keyedStream.process(new TempIncreaseAlertFunction());
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> processedStream2 = keyedStream.flatMap(new TempChangeAlert(2));
//        SingleOutputStreamOperator<Tuple3<String, Double, Double>> processedStream3 = keyedStream.process(new TempChangeAlert2(2));

        stream.print("input data");
        processedStream2.print("process data");

        env.execute("WindowTest");
    }
}

class TempChangeAlert extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

    private double threshold;
    private ValueState<Double> lastTempState = null;

    public TempChangeAlert(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));
    }

    @Override
    public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
        // 获取上次的温度值
        Double lastTemp = lastTempState.value();
        if (lastTemp != null) {
            double diff = Math.abs(value.getTemperature() - lastTemp);
            if (diff > threshold) {
                out.collect(new Tuple3<>(value.getId(), lastTemp, value.getTemperature()));
            }
        }
        lastTempState.update(value.getTemperature());
    }
}

class TempChangeAlert2 extends KeyedProcessFunction<String, SensorReading, Tuple3<String, Double, Double>> {

    private double threshold;
    private ValueState<Double> lastTempState = null;

    public TempChangeAlert2(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));
    }

    @Override
    public void processElement(SensorReading value, Context ctx, Collector<Tuple3<String, Double, Double>> out) throws Exception {
        // 获取上次的温度值
        Double lastTemp = lastTempState.value();
        if (lastTemp != null) {
            double diff = Math.abs(value.getTemperature() - lastTemp);
            if (diff > threshold) {
                out.collect(new Tuple3<>(value.getId(), lastTemp, value.getTemperature()));
            }
        }
        lastTempState.update(value.getTemperature());
    }
}

/**
 * 需求：监控温度传感器的值，如果在一秒钟之内(processing time)连续上升，则报警
 */
class TempIncreaseAlertFunction extends KeyedProcessFunction<String, SensorReading, String> {

    // 定义一个状态，用来保存上一个数据的温度值
    private ValueState<Double> lastTemp = null;
    // 定义一个状态，用来保存定时器的时间戳
    private ValueState<Long> currentTimer = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("currentTimer", Long.class));
    }

    @Override
    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
        // 先取出上一个温度值
        Double preTemp = lastTemp.value();
        // 更新温度值
        lastTemp.update(value.getTemperature());
        Long curTimerTs = currentTimer.value();

        // 如果温度下降，或是第一条数据，删除定时器并清空状态
        if (preTemp == null || value.getTemperature() < preTemp) {
            if (curTimerTs != null) {
                ctx.timerService().deleteProcessingTimeTimer(curTimerTs);
            }
            System.out.println("xxxxxxxxx " + value);
            currentTimer.clear();
        } else if (value.getTemperature() > preTemp && curTimerTs == null) {  // 温度上升且没有设过定时器，则注册定时器
            long timeTs = ctx.timerService().currentProcessingTime() + 2000L;
            ctx.timerService().registerProcessingTimeTimer(timeTs);
            System.out.println("========> " + value);
            currentTimer.update(timeTs);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 输出报警信息
        out.collect(ctx.getCurrentKey() + " " + ctx.timestamp() + " 温度连续上升");
        currentTimer.clear();
    }
}

package com.scz.flk.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by shen on 2020/2/6.
 */
public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of(1, 3), Tuple2.of(1, 5), Tuple2.of(1, 7), Tuple2.of(1, 4), Tuple2.of(1, 2))
                .keyBy(0)
                .flatMap(new RichFlatMapFunction<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>() {
                    // ValueState状态句柄，第一个值为count，第二个值为sum
                    private  transient ValueState<Tuple2<Integer,Integer>> sum;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Tuple2<Integer,Integer>> descriptor = new ValueStateDescriptor<Tuple2<Integer, Integer>>(
                                "average", // 状态名称
                                TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {}), // 状态类型
                                Tuple2.of(0, 0)); // 状态默认值
                        sum = getRuntimeContext().getState(descriptor);
                    }
                    @Override
                    public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        Tuple2<Integer, Integer> currentSum = sum.value(); // 获取当前状态值
                        currentSum.f0 += 1; // 更新
                        currentSum.f1 += value.f1;
                        sum.update(currentSum); // 更新状态值
                        if (currentSum.f0 >= 2) { // 如果count>=2，清空状态值，重新计算
                            out.collect(new Tuple2<>(value.f0, currentSum.f1 / currentSum.f0));
                            sum.clear();
                        }
                    }
                }).print(); // the printed output will be (1,4) and (1,5)
        env.execute("StateTest");
    }
}

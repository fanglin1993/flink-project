package com.scz.flk.stream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by shen on 2019/12/22.
 */
public class DataStreamTransformationApp {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> customDS = env.addSource(new CustomRichParallelSourceFunction());
        customDS.print();
        env.execute("source");
    }
}

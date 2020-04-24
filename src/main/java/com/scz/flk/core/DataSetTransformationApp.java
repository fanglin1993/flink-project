package com.scz.flk.core;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import java.util.Arrays;

/**
 * Created by shen on 2019/12/22.
 */
public class DataSetTransformationApp {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        joinFunction(env);
//        crossFunction(env);
    }

    public static void joinFunction(ExecutionEnvironment env) throws Exception {
        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(Arrays.asList(
                new Tuple2<>(1, "PK哥"), new Tuple2<>(2, "J哥"),
                new Tuple2<>(3, "小队长"), new Tuple2<>(4, "猪头呼")
        ));
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(Arrays.asList(
                new Tuple2<>(1, "北京"), new Tuple2<>(2, "上海"),
                new Tuple2<>(3, "成都"), new Tuple2<>(5, "杭州")
        ));
        data1.join(data2).where(0).equalTo(0).map(new MapFunction<Tuple2<Tuple2<Integer,String>,Tuple2<Integer,String>>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> map(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> value) throws Exception {
                return new Tuple3<>(value.f0.f0, value.f0.f1, value.f1.f1);
            }
        });
        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> result = data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first == null) {
                    return new Tuple3<>(second.f0, "-", second.f1);
                }
                if (second == null) {
                    return new Tuple3<>(first.f0, first.f1, "-");
                }
                return new Tuple3<>(first.f0, first.f1, second.f1);
            }
        });
        String outPath = "F:/index/result/flink.csv";
        result.setParallelism(1).writeAsCsv(outPath, WriteMode.OVERWRITE);
        env.execute("write file");
    }

    public static void crossFunction(ExecutionEnvironment env) throws Exception {
        DataSource<String> data1 = env.fromCollection(Arrays.asList("曼联","曼城"));
        DataSource<Integer> data2 = env.fromCollection(Arrays.asList(3, 1, 0));
        data1.cross(data2).print();
    }

}

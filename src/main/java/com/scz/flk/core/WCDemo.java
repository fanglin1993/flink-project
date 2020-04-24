package com.scz.flk.core;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.operators.Order;

import java.util.StringTokenizer;
import java.util.function.Consumer;

/**
 * Created by shen on 2019/12/21.
 */
public class WCDemo {
    public static void main(String[] args) {
        final ExecutionEnvironment env =ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> fileDS = env.readTextFile("data/wc.txt");
        FlatMapOperator<String, Tuple2<String, Integer>> oneForWordDS = fileDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                StringTokenizer tokenizer = new StringTokenizer(value);
                while (tokenizer.hasMoreTokens()) {
                    out.collect(new Tuple2(tokenizer.nextToken(), 1));
                }
            }
        });
        AggregateOperator<Tuple2<String, Integer>> wcAO = oneForWordDS.groupBy(0).sum(1).setParallelism(1);
        SortPartitionOperator<Tuple2<String, Integer>> sortedWcAO = wcAO.sortPartition(1, Order.DESCENDING);
        try {
            sortedWcAO.collect().forEach(new Consumer<Tuple2<String, Integer>>() {
                @Override
                public void accept(Tuple2<String, Integer> tuple) {
                    System.out.println(Thread.currentThread().getId() + " ==> " + tuple);
                }
            });
//            sortedWcAO.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/**
 (yarn,3)
 (hdfs,1)
 (streaming,1)
 (hadoop,3)
 (hive,1)
 (on,2)
 (spark,3)
 (mapreduce,2)
 (sql,2)
 */
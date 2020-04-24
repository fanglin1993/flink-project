package com.scz.flk.core;

import com.scz.flk.model.OrderEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by shen on 2019/12/22.
 */
public class TestSource {
    public static void main(String[] args) throws Exception {
        fromCollectionDemo();
//        readCsvFileDemo();

    }


    public static void fromCollectionDemo() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple2<String, Integer>> collectionDS =  env.fromCollection(Arrays.asList(
                new Tuple2<>("Tom", 23),
                new Tuple2<>("Jack", 25),
                new Tuple2<>("Smith", 28),
                new Tuple2<>("Alan", 18),
                new Tuple2<>("Smith", 26)
        ));
        collectionDS.distinct(0).print();
    }


    public static void readCsvFileDemo() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        CsvReader reader = env.readCsvFile("data/flink/OrderLog.csv").fieldDelimiter(",").includeFields(true, true, true, true);
        DataSource<Tuple4<Long, String, String, Long>> csvDS = reader.types(Long.class, String.class, String.class, Long.class);
        MapOperator<Tuple4<Long,String,String,Long>, OrderEvent> orderDS = csvDS.map(new MapFunction<Tuple4<Long,String,String,Long>, OrderEvent>() {
            @Override
            public OrderEvent map(Tuple4<Long, String, String, Long> value) throws Exception {
                return new OrderEvent(value.f0, value.f1, value.f2, value.f3);
            }
        });

        orderDS.groupBy(new KeySelector<OrderEvent, String>() {
            @Override
            public String getKey(OrderEvent value) throws Exception {
                return value.getEventType();
            }
        }).sortGroup(new KeySelector<OrderEvent, Long>() {
            @Override
            public Long getKey(OrderEvent value) throws Exception {
                return value.getEventTime();
            }
        }, Order.ASCENDING).first(3).print();

        MapPartitionOperator<OrderEvent, Tuple2<String, Integer>> sumByEventTypeDS = orderDS.mapPartition(new MapPartitionFunction<OrderEvent, Tuple2<String, Integer>>() {
            @Override
            public void mapPartition(Iterable<OrderEvent> values, Collector<Tuple2<String, Integer>> out) throws Exception {
                Map<String, Integer> result = new HashMap<>();
                for (OrderEvent order : values) {
                    int cnt = 1;
                    if (result.containsKey(order.getEventType())) {
                        cnt += result.get(order.getEventType());
                    }
                    result.put(order.getEventType(), cnt);
                }
                for (Map.Entry<String, Integer> entry : result.entrySet()) {
                    Tuple2<String, Integer> tuple = new Tuple2<>(entry.getKey(), entry.getValue());
                    System.out.println(Thread.currentThread().getId() + " ==> " + tuple);
                    out.collect(tuple);
                }
            }
        });
//        sumByEventTypeDS.print();

        FilterOperator<OrderEvent> filteredDS = orderDS.filter(new FilterFunction<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return value.getTxId().length() > 0;
            }
        });
//        filteredDS.print();
    }
}
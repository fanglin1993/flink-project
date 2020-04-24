package com.scz.flk.table;

import com.scz.flk.model.SensorReading;
import com.scz.flk.source.SensorSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Created by shen on 2019/12/26.
 */
public class StreamTableTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource(100, true));
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        Table table = tableEnv.fromDataStream(stream);
        tableEnv.registerTable("SensorTable", table);
        Table resultTable = tableEnv.sqlQuery("select id, avg(temperature) as avg_temp, count(1) as cnt from SensorTable group by id");
        DataStream<Tuple2<Boolean, Row>> resultDS = tableEnv.toRetractStream(resultTable, Row.class);
        stream.print("input =");
        resultDS.print("->Retract result");

        env.execute("StreamTableTest");
    }
}

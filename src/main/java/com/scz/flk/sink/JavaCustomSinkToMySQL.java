package com.scz.flk.sink;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by shen on 2019/12/23.
 */
public class JavaCustomSinkToMySQL {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Student> studentDS = source.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] split = value.split(" ");
                Student student = new Student();
                student.setId(Integer.parseInt(split[0]));
                student.setName(split[1]);
                student.setAge(Integer.parseInt(split[2]));
                return student;
            }
        });
        studentDS.addSink(new SinkToMySQL());
        env.execute("sink test");
    }
}

package com.scz.flk.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Created by shen on 2019/12/23.
 */
public class SinkToMySQL extends RichSinkFunction<Student>{

    private Connection connection;
    private PreparedStatement pstmt;

    private void init() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://localhost:3306/test";
            this.connection = DriverManager.getConnection(url,"root","123456");
            String sql = "insert into student(name,age) values (?,?)";
            this.pstmt = this.connection.prepareStatement(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 在open方法中建立connection
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.init();
        System.out.println("open");
    }

    // 每条记录插入时调用一次
    public void invoke(Student value, Context context) throws Exception {
        System.out.println("invoke~~~~~~~~~");
        // 未前面的占位符赋值
        pstmt.setString(1, value.getName());
        pstmt.setInt(2, value.getAge());
        pstmt.executeUpdate();
    }

    /**
     * 在close方法中要释放资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if(pstmt != null) {
            System.out.println("pstmt close...");
            pstmt.close();
        }
        if(connection != null) {
            connection.close();
            System.out.println("disconnect...");
        }
    }
}
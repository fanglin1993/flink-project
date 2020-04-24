package com.scz.flk.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Created by shen on 2019/12/23.
 */
public class JavaTableSQLAPI {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);
        DataSet<Sales> csv = env.readCsvFile("data/flink/sales.csv")
                .ignoreFirstLine()
                .pojoType(Sales.class,"transactionId","customerId","itemId","amountPaid");
        //csv.print();
        Table sales = tableEnv.fromDataSet(csv);
        tableEnv.registerTable("sales", sales);
        Table resultTable = tableEnv.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId");
        DataSet<Row> result = tableEnv.toDataSet(resultTable, Row.class);
        result.print();
    }

    public static class Sales{
        public String transactionId;
        public String customerId;
        public String itemId;
        public Double  amountPaid;
    }
}

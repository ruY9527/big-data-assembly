package com.iyang.base.tables;

import com.iyang.base.functions.Top2Function;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/****
 * author: BaoYang
 * date: 2023/6/30
 * desc:
 ***/
public class MyTableAggregate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> numDs = env.fromElements(3, 6, 12, 5, 8, 9, 4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table numTable = tableEnv.fromDataStream(numDs, $("num"));
        tableEnv.createTemporaryFunction("Top2Function", Top2Function.class);

        numTable.flatAggregate(call("Top2Function", $("num")).as("value","rank"))
                .select($("value"), $("rank")).execute().print();

    }

}

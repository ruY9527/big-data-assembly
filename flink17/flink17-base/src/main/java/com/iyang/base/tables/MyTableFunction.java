package com.iyang.base.tables;

import com.iyang.base.functions.SplitFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/****
 * author: BaoYang
 * date: 2023/6/30
 * desc:
 ***/
public class MyTableFunction {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> strDs = env.fromElements(
                "hello flink",
                "hello java",
                "hello world"
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table sensorTable = tableEnv.fromDataStream(strDs, $("words"));
        tableEnv.createTemporaryView("str", sensorTable);

        tableEnv.createFunction("SplitFunction", SplitFunction.class);
        tableEnv.sqlQuery(
                "select words,newWord,newLength from str left join lateral table(SplitFunction(words))  as T(newWord,newLength) on true"
        ).execute().print();
        /*tableEnv.sqlQuery(
                "select words,word,length from str,lateral table(SplitFunction(words))"
        ).execute().print();*/

    }


}

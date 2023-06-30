package com.iyang.base.tables;

import com.iyang.base.functions.WeightedAvgTable;
import org.apache.flink.api.java.tuple.Tuple3;
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
public class MyAggregateFunction {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, Integer, Integer>> sensorDs = env.fromElements(
                Tuple3.of("zs", 80, 3),
                Tuple3.of("zs", 90, 4),
                Tuple3.of("zs", 95, 4),
                Tuple3.of("ls", 75, 4),
                Tuple3.of("ls", 65, 4),
                Tuple3.of("ls", 85, 4)
        );

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        Table scoreWeightTable = tableEnvironment.fromDataStream(sensorDs, $("f0").as("name"),
                $("f1").as("score"), $("f2").as("weight"));

        tableEnvironment.createTemporaryView("scores", scoreWeightTable);
        tableEnvironment.createTemporaryFunction("WeightedAvgTable", WeightedAvgTable.class);

        tableEnvironment.sqlQuery(
                "select name,WeightedAvgTable(score,weight)  from scores group by name"
        ).execute().print();


    }


}

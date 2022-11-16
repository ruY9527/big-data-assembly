package com.iyang.table;

import com.iyang.table.objs.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/***
 * @author: baoyang
 * @data: 2022/11/16
 * @desc:
 ***/
public class TableDyByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStreamSource<Event> elements = environment.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L),
                new Event("Bob", "./prod?id=3", 90 * 1000L),
                new Event("Alice", "./prod?id=7", 105 * 1000L)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);
        tableEnv.createTemporaryView("EventTable", elements, $("user"), $("url"), $("timestamp").as("ts"));

        Table urlSqlQuery = tableEnv.sqlQuery("select user,count(url) from EventTable group by user");

        tableEnv.toChangelogStream(urlSqlQuery).print("count");
        environment.execute("running....");
    }

}

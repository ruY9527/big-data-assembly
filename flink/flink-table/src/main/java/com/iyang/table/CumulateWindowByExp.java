package com.iyang.table;

import com.iyang.table.objs.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/***
 * @author: baoyang
 * @data: 2022/11/16
 * @desc:
 ***/
public class CumulateWindowByExp {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<Event> streamOperator = environment.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);
        Table table = tableEnv.fromDataStream(
                streamOperator, $("user"), $("url"), $("timestamp").rowtime().as("ts")
        );

        tableEnv.createTemporaryView("EventTable", table);

        Table sqlQuery = tableEnv.sqlQuery("select user,window_end as endT,count(url) as cnt from TABLE(" +
                " CUMULATE( TABLE EventTable," +
                " DESCRIPTOR(ts)," +
                " INTERVAl '30' MINUTE, " +
                " INTERVAL '1' HOUR)) group by user,window_start,window_end");

        tableEnv.toDataStream(sqlQuery).print("result:");
        environment.execute("runing...");

    }

}

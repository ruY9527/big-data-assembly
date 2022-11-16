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
public class WindowTopNByExp {

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

        Table table = tableEnv.fromDataStream(streamOperator, $("user"), $("url"), $("timestamp").rowtime().as("ts"));
        tableEnv.createTemporaryView("EventTable", table);

        // 基于 ts 开启一小时的滚动窗口
        String subQuery = "select window_start, window_end, user,count(url) as cnt from TABLE( " +
                " TUMBLE( TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOUR)) GROUP BY window_start,window_end,user";

        String topQuery = "select * from ( select * , ROW_NUMBER() OVER(PARTITION BY window_start,window_end order by cnt desc) as row_num" +
                " from ( " + subQuery + ")) where row_num <= 2" ;

        Table sqlQuery = tableEnv.sqlQuery(topQuery);
        tableEnv.toDataStream(sqlQuery).print("topQuery");

        environment.execute("runing...");
    }

}

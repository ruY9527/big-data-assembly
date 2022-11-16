package com.iyang.table;

import com.iyang.table.objs.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/***
 * @author: baoyang
 * @data: 2022/11/16
 * @desc:
 ***/
public class TableChangeLogByExp {

    public static void main(String[] args) throws Exception {

        // +I , -U 和 +U 三种 RowKind, 分别代表 (+I)INSERT(插入) , (-U)UPDATE_BEFORE(更新前) 和 (+U)UPDATE_AFTER(更新后)
        // 获取流环境
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

        // 获取表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // 将数据流转化为表
        tableEnvironment.createTemporaryView("EventTable", elements);

        Table bobQuery = tableEnvironment.sqlQuery("select user,url from EventTable where user = 'Bob'");
        Table countQuery = tableEnvironment.sqlQuery("select user,count(url) from EventTable group by user");

        tableEnvironment.toDataStream(bobQuery).print("Bob print");
        // changelog stream 中会出现对应二条数据,分别表示之前数据的失效和新数据的生效
        tableEnvironment.toChangelogStream(countQuery).print("count print");

        environment.execute("runing");
    }

}

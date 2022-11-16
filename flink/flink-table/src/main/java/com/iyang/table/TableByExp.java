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
public class TableByExp {

    public static void main(String[] args) throws Exception {

        // 获取执行环境流
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 模拟数据来源
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
        Table table = tableEnvironment.fromDataStream(elements);

        System.out.println("查询sql语句");
        // select url,user from UnnamedTable$0
        System.out.println("select url,user from " + table);
        System.out.println("打印查询sql完毕");

        // 用执行SQL的方式提取数据
        // +I 的标志,插入数据
        // Table sqlQuery = tableEnvironment.sqlQuery("select url,user from " + table + " where user = 'Bob'");
        // Table sqlQuery = table.select($("url"), $("user"));
        Table sqlQuery = tableEnvironment.sqlQuery("select user, count(url) from "+ table +" group by user");

        // 将表转换成数据流,打印输出
        //tableEnvironment.toDataStream(sqlQuery).print();
        tableEnvironment.toChangelogStream(sqlQuery).print();
        environment.execute("runing table...");

    }

}

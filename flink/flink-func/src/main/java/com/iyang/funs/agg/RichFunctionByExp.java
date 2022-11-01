package com.iyang.funs.agg;

import com.iyang.funs.datas.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class RichFunctionByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );

        clicks.map(new RichMapFunction<Event, Long>(){

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化连接操作,比如 MYSQL 连接等

                super.open(parameters);
                System.out.println("索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 任务开始");
            }

            @Override
            public void close() throws Exception {
                // 清理工作,比如对连接进行关闭操作等

                super.close();
                System.out.println("索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务结束");
            }

            @Override
            public Long map(Event value) throws Exception {
                return value.timestamp;
            }
        }).print();

        env.execute("runing ....");

    }

}

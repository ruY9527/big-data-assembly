package com.iyang.base.partitions;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class ShuffleStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 7777);
        stream.shuffle().print();

        // 重分区
        // stream.rebalance().print();

        env.execute();

    }

}

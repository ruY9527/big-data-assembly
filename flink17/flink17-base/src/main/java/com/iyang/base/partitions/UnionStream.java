package com.iyang.base.partitions;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class UnionStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> ds2 = env.fromElements(2, 2, 3);
        DataStreamSource<String> ds3 = env.fromElements("3", "5", "7");

        ds1.union(ds2,ds3.map(Integer::valueOf)).print();

        env.execute();

    }

}

package com.iyang.base.sources;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class UnionStreams {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> ds1 = env.fromCollection(Arrays.asList(1, 2, 3));
        DataStreamSource<Integer> ds2 = env.fromCollection(Arrays.asList(2, 3, 4));
        DataStreamSource<String> ds3 = env.fromCollection(Arrays.asList("2", "4", "5"));

        ds1.union(ds2,ds3.map(Integer::valueOf)).print();

        env.execute();

    }

}

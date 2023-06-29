package com.iyang.base.partitions;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc: 分流
 ***/
public class SplitStreams {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Integer> ds = environment.socketTextStream("localhost", 7788)
                .map(Integer::valueOf);

        SingleOutputStreamOperator<Integer> ds1 = ds.filter(x -> x % 2 == 0);
        SingleOutputStreamOperator<Integer> ds2 = ds.filter(x -> x % 2 == 1);

        ds1.print("偶数");
        ds2.print("奇数");

        environment.execute();

    }

}

package com.iyang.base.partitions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class CustomerPartition {

    public static void main(String[] args) throws Exception {

        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        environment.setParallelism(2);

        DataStreamSource<String> source = environment.socketTextStream("localhost", 7777);
        DataStream<String> myDs = source.partitionCustom(new UserPartition(), value -> value);

        myDs.print();
        environment.execute();

    }

}

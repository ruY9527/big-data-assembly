package com.iyang.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/****
 * author: BaoYang
 * date: 2023/6/11
 * desc: 流式开发处理
 ***/
public class StreamWordCount {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = environment.readTextFile("/home/luohong/coding/java/github_self/big-data-assembly/flink17/flink17-base/files/words.txt");

        SingleOutputStreamOperator<Tuple2<String, Long>> streamOperator = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {

                String[] strings = s.split(" ");
                Arrays.stream(strings).forEach(v -> collector.collect(Tuple2.of(v, 1L)));

            }
        }).keyBy(data -> data.f0).sum(1);


        streamOperator.print();

        environment.execute();
    }


}

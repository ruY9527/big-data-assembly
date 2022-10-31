package com.iyang.flinks.sockets;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/***
 * @author: baoyang
 * @data: 2022/10/31
 * @desc:
 ***/
public class WordCountSocket {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 需要 nc 来进行开通数据通道
        DataStreamSource<String> dataStreamSource = environment.socketTextStream("172.0.0.1", 9988);
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

                String[] fileds = s.split(",");
                for (String filed : fileds) {
                    collector.collect(new Tuple2<>(filed, 1));
                }

            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> wordCounts = operator.keyBy(0);
        wordCounts.print();
        environment.execute("word counts...");
    }

}

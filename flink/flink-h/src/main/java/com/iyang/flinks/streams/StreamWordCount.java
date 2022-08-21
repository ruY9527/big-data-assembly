package com.iyang.flinks.streams;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/***
 * @author: baoyang
 * @data: 2022/8/21
 * @desc: 使用 nc -lk 来进行模拟
 ***/
public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineDss = env.socketTextStream("bd211", 9999);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDss.flatMap((String line, Collector<String> words) -> {
            Arrays.stream(line.split(" ")).forEach(words::collect);
        }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> wordAndOneKs = wordAndOne.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKs.sum(1);
        sum.print();

        env.execute();

    }

}

package com.iyang.flinks;

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
 * @data: 2022/8/14
 * @desc:
 ***/
public class TestFileStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> testEnv = env.socketTextStream("bd211", 7777);
        SingleOutputStreamOperator<Tuple2<String, Long>> words = testEnv.flatMap((String line, Collector<String> wordss) ->
            Arrays.stream(line.split(" ")).forEach(wordss::collect)
        ).returns(Types.STRING).map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        KeyedStream<Tuple2<String, Long>, String> wordKeys = words.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordKeys.sum(1);
        sum.print();

        env.execute();

    }

}

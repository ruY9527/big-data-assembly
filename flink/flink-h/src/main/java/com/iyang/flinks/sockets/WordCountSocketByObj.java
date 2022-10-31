package com.iyang.flinks.sockets;

import com.iyang.flinks.datas.WordCountObj;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/***
 * @author: baoyang
 * @data: 2022/10/31
 * @desc:
 ***/
public class WordCountSocketByObj {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("172.21.129.217", 8899);
        SingleOutputStreamOperator<WordCountObj> operator = socketTextStream.flatMap(new FlatMapFunction<String, WordCountObj>() {

            @Override
            public void flatMap(String s, Collector<WordCountObj> collector) throws Exception {

                String[] fileds = s.split(",");
                for (String filed : fileds) {
                    collector.collect(new WordCountObj(filed, 1));
                }

            }
            // 指定 keyBy 以及 sum 的字段
        }).keyBy("word").sum("count");
        operator.print();
        env.execute("word obj count...");

    }

}

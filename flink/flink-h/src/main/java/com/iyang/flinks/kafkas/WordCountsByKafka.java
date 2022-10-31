package com.iyang.flinks.kafkas;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/***
 * @author: baoyang
 * @data: 2022/10/31
 * @desc:
 ***/
public class WordCountsByKafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topicName = "topics";

        Properties consumerPro = new Properties();
        consumerPro.setProperty("bootstrap.servers", "bd210:9092");
        consumerPro.setProperty("group.id", "topics_consumers");

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>(topicName,
                new SimpleStringSchema(), consumerPro);
        DataStreamSource<String> data = env.addSource(myConsumer).setParallelism(3);

        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] fields = s.split(",");
                for (String field : fields) {
                    collector.collect(Tuple2.of(field, 1));
                }
            }
        }).setParallelism(2);

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = operator.keyBy(0).sum(1).setParallelism(2);
        streamOperator.map(tuple -> tuple.toString()).setParallelism(2).print().setParallelism(1);
        env.execute("word count from kafka");
    }

}

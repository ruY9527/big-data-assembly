package com.iyang.funs.sink;

import com.alibaba.fastjson.JSONObject;
import com.iyang.funs.datas.Event;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class SinkToKafkaByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "udp215:9092");

        DataStreamSource<String> streamSource = env.fromElements(
                JSONObject.toJSONString(new Event("Mary", "./home", 1000L))
               );

        // sink 给写入到 kafka 中
        streamSource.addSink(new FlinkKafkaProducer<String>("baoyang_test_topic", new SimpleStringSchema(), properties));
        env.execute("toKafkaRuning....");
    }

}

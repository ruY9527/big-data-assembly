package com.iyang.base.sinks;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class SinkToKafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        DataStreamSource<String> sensorDs = env.socketTextStream("localhost", 8899);

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder().setBootstrapServers("udp215:9092,udp216:9092,udp218:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder().setTopic("test01")
                        .setValueSerializationSchema(new SimpleStringSchema()).build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("baoyang-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "")
                .build();

        sensorDs.sinkTo(kafkaSink);
        env.execute();


    }


}

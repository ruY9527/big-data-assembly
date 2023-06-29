package com.iyang.base.sinks;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class SelfKafkaRecordSerializationSchema implements KafkaRecordSerializationSchema<String> {

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {

        String[] datas = element.split(",");
        byte[] key = datas[0].getBytes(StandardCharsets.UTF_8);
        byte[] value = datas[1].getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>("test01", key, value);
    }
}

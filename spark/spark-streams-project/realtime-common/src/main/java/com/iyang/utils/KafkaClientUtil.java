package com.iyang.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/****
 * author: BaoYang
 * date: 2023/7/12
 * desc:
 ***/
public class KafkaClientUtil {

    //构造一个单例的生产者
    public static Producer<String,String> producer;

    //在类加载时执行，类只会加载一次，只执行一次
    static {
        producer = getProducer();
    }

    // 生成string类型的value到kafka
    public static void sendDataToKafka(String topic,String value){
        producer.send(new ProducerRecord<String,String>(topic,value));
    }


    //构造producer
    private static Producer<String,String> getProducer(){

        //提供producer的配置
        Properties properties = new Properties();

        // 参考: ProducerConfig
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,PropertiesUtil.getProperty("kafka.broker.list"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,PropertiesUtil.getProperty("key.serializer"));
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,PropertiesUtil.getProperty("value.serializer"));

        // 基于配置构造producer
        return new KafkaProducer<String, String>(properties);

    }

}

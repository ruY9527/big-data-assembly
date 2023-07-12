package com.iyang.obj.streams

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object SparkStreamByKafka {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "udp215",
      ConsumerConfig.GROUP_ID_CONFIG -> "baoyang",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("testTopic"), kafkaPara)
    )

    val valueDStream: DStream[String] = kafkaDStream.map(r => r.value())
    valueDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print();
    ssc.start()
    ssc.awaitTermination()
  }

}

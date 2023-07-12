package com.iyang.jdbc.utils

import com.iyang.utils.PropertiesUtil
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/** **
 * author: BaoYang
 * date: 2023/7/12
 * desc: 
 * * */
object MyKafkaUtil {

  val borkerList: String = PropertiesUtil.getProperty("kafka.broker.list")


  def getKafkaStream(topics:Array[String],
                     ssc:StreamingContext,
                     groupId:String,
                     saveOffsetToMySQL:Boolean = false,
                     offsetMap: Map[TopicPartition, Long] = null,
                     ifAutoCommit:String ="false"): Unit = {
    val kafkaParam = Map(
      "bootstrap.servers" -> borkerList,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> ifAutoCommit
    )

    if (saveOffsetToMySQL) {
      KafkaUtils.createDirectStream[String,String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](topics,kafkaParam,offsetMap)
      )
    } else {
      KafkaUtils.createDirectStream[String,String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](topics,kafkaParam)
      )
    }

  }


}

package com.iyang.base

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.google.gson.Gson
import com.iyang.utils.{KafkaClientUtil, TopicConstant}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD

import java.util

/** **
 * author: BaoYang
 * date: 2023/7/12
 * desc: 
 * * */
object LogDiversionApp extends BaseApp {

  override var groupName: String = "realtime220212"

  override var batchDuration: Int = 10

  override var appName: String = "LogDiversionApp"

  override var topic: String = TopicConstant.ORIGINAL_LOG

  def parseJsonArray(map:JSONObject,
                     result:util.Map[String, AnyRef],
                     topic:String,
                     gson:Gson
                    ): Unit = {

    val arrayStr: String = map.getString("actions")
    val jSONArray: JSONArray = JSON.parseArray(arrayStr)

    for(i <- 0 until jSONArray.size()){

      val eleMap: util.Map[String, AnyRef] = JSON.parseObject(jSONArray.getString(i)).getInnerMap
      result.putAll(eleMap)
      KafkaClientUtil.sendDataToKafka(topic, gson.toJson(result))
    }
  }


  def parseLog(rdd:RDD[ConsumerRecord[String,String]]): Unit = {

    val rdd1: RDD[String]  = rdd.map(record => record.value())
    rdd1.foreachPartition(partition => {

      val gson = new Gson()
      partition.foreach(logStr => {

        val map:JSONObject = JSON.parseObject(logStr)
        val commonStr: String  = map.getString("common")
        val resultMap: JSONObject = JSON.parseObject(commonStr)
        resultMap.put("rs", map.getLong("ts"))
        val result: util.Map[String, AnyRef] = resultMap.getInnerMap

        if (map.containsKey("start") && !map.containsKey("err")) {
          val startMap: util.Map[String, AnyRef] = JSON.parseObject(map.getString("start")).getInnerMap
          resultMap.putAll(startMap)
          KafkaClientUtil.sendDataToKafka(TopicConstant.STARTUP_LOG, gson.toJson(result))
        } else if(map.containsKey("actions") && !map.containsKey("err")) {
          val pageMap: util.Map[String, AnyRef] = JSON.parseObject(map.getString("page")).getInnerMap
          result.putAll(pageMap)
          parseJsonArray(map, result, TopicConstant.ACTION_LOG, gson)
        }
      })
    })
    // 
    // KafkaClientUtil.flush()
  }


}

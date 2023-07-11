package com.iyang.obj.spark3.checkpoints

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object HdfsCheckPoint {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","hadoop")

    val conf:SparkConf = new SparkConf().setAppName("HdfsCheckPoint").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    sc.setCheckpointDir("hdfs://udp218:8020/tmp/checkpoint")

    val lineRdd:RDD[String] = sc.textFile("/home/luohong/logs/rocketmqlogs")
    val wordRdd:RDD[String] = lineRdd.flatMap(line => line.split(" "))

    val wordToOneRdd: RDD[(String, Long)] = wordRdd.map(word => {
      (word, System.currentTimeMillis())
    })

    wordToOneRdd.cache()
    wordToOneRdd.collect().foreach(println)
    sc.stop()
  }
}

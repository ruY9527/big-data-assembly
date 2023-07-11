package com.iyang.obj.spark3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc:  bin/spark-submit \
  --class com.iyang.obj.spark3.WordCount \
  --master yarn \
  ./WordCount.jar \
  hdfs://hadoop102:8020/input \
  hdfs://hadoop102:8020/output
   * * */
object WordCount {

  def main(args: Array[String]): Unit = {
    val fielPath = "/home/luohong/logs/rocketmqlogs"

    val startTime = System.currentTimeMillis()

    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lineRdd:RDD[String] = sc.textFile(fielPath)

    val wordRdd:RDD[String] = lineRdd.flatMap(_.split(" "))
    val wordToOneRdd:RDD[(String,Int)] = wordRdd.map((_, 1))

    val wordToSumRdd: RDD[(String, Int)] = wordToOneRdd.reduceByKey(_ + _)

    wordToSumRdd.collect().foreach(println)
    sc.stop()

    println("总共消耗时间:" + (System.currentTimeMillis() - startTime))
  }

}

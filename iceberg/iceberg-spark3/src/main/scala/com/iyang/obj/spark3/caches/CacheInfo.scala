package com.iyang.obj.spark3.caches

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object CacheInfo {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("CacheInfo").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val lineRdd:RDD[String] = sc.textFile("1.text")
    val wordLine:RDD[String] = lineRdd.flatMap(line => line.split(" "))

    val wordToOneRdd: RDD[(String, Int)] = wordLine.map {
      word =>
        println("**********************")
        (word, 1)
    }

    // 打印血缘关系
    println(wordToOneRdd.toDebugString)

    wordToOneRdd.cache()
    wordToOneRdd.collect().foreach(println)

    println(wordToOneRdd.toDebugString)
    println("-------------------------------------")

    wordToOneRdd.collect().foreach(println)
    Thread.sleep(1000000)
    sc.stop()
  }

}

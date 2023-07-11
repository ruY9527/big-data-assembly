package com.iyang.obj.spark3.infos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object LineageInfo {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("LineageInfo").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val fileRdd:RDD[String] = sc.textFile("1.txt")
    println(fileRdd.toDebugString)
    println("-------------------------------------")

    val wordRdd:RDD[String] = fileRdd.flatMap(_.split(" "))
    println(wordRdd.toDebugString)
    println("-------------------------------------")

    val mapRdd:RDD[(String,Int)] = wordRdd.map((_, 1))
    println(mapRdd.toDebugString)
    println("-------------------------------------")

    val resultRdd:RDD[(String,Int)] = mapRdd.reduceByKey(_ + _)
    println(resultRdd.toDebugString)

    resultRdd.count();
    sc.stop()
  }

}

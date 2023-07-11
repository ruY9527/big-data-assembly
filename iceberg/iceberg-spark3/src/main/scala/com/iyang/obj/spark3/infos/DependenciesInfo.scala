package com.iyang.obj.spark3.infos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object DependenciesInfo {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("DependenciesInfo").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val fileRdd:RDD[String] = sc.textFile("1.text")
    println(fileRdd.dependencies)
    println("-------------------------------------")

    val wordRdd:RDD[String] = fileRdd.flatMap(_.split(" "))
    println(wordRdd.dependencies)
    println("-------------------------------------")

    val mapRdd:RDD[(String,Int)] = wordRdd.map((_, 1))
    println(mapRdd.dependencies)
    println("-------------------------------------")

    val resultRDD: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)
    println(resultRDD.dependencies)
    resultRDD.collect();

    sc.stop()
  }

}

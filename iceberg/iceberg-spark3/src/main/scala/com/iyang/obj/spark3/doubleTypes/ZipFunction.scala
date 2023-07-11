package com.iyang.obj.spark3.doubleTypes

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object ZipFunction {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("ZipFunction").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val rdd1:RDD[Int] = sc.makeRDD(Array(1, 2, 3), 3)
    val rdd2:RDD[String] = sc.makeRDD(Array("a", "b", "c"), 3)

    rdd1.zip(rdd2).collect().foreach(println)

    rdd2.zip(rdd1).collect().foreach(println)
    sc.stop()
  }

}

package com.iyang.obj.spark3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object DistinctFunction {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("DistinctFunction").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val distinctRdd:RDD[Int] = sc.makeRDD(List(1, 2, 1, 5, 2, 9, 6, 1))
    distinctRdd.distinct(2).collect().foreach(println)

    sc.stop()
  }

}

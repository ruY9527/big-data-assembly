package com.iyang.obj.spark3.doubleTypes

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object UnionFunction {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("UnionFunction").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val rdd1:RDD[Int] = sc.makeRDD(1 to 4)
    val rdd2:RDD[Int] = sc.makeRDD(4 to 8)

    rdd1.union(rdd2).collect().foreach(println)
    sc.stop()
  }

}

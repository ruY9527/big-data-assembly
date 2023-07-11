package com.iyang.obj.spark3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object ListCommonSpark {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val list = List(1, 2, 3, 4, 5, 6)
    val intRdd:RDD[Int] = sc.parallelize(list)

    intRdd.collect().foreach(println)

    val intRDDRepeate:RDD[Int] = sc.makeRDD(list)
    intRDDRepeate.collect().foreach(println)

    sc.stop()
  }

}

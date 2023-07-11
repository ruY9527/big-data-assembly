package com.iyang.obj.spark3.partitions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object CogroupBase {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("CogroupBase").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val rdd1: RDD[(Int,String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((1,4),(2,5),(4,6)))

    rdd1.cogroup(rdd2).collect().foreach(println)
    sc.stop()
  }

}

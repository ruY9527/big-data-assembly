package com.iyang.obj.spark3.actions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object CountByKey {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("CountByKey").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val rdd:RDD[(Int,String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
    val result:collection.Map[Int,Long] = rdd.countByKey()

    println(result)
    sc.stop()
  }

}

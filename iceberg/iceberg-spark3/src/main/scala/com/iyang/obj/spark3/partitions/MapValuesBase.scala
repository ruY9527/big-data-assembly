package com.iyang.obj.spark3.partitions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object MapValuesBase {


  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("MapValuesBase").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val rdd:RDD[(Int,String)] = sc.makeRDD(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")))
    rdd.mapValues(_+"||||||").collect().foreach(println)

    sc.stop()
  }

}

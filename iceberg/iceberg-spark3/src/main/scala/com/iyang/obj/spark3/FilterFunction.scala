package com.iyang.obj.spark3

import com.iyang.obj.spark3.utils.RddUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object FilterFunction {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("FilterFunction").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val intRdd: RDD[Int] = RddUtils.createRddData(sc)

    val filterRdd:RDD[Int] = intRdd.filter(_ % 2 == 0)
    filterRdd.collect().foreach(println)

    sc.stop()
  }

}

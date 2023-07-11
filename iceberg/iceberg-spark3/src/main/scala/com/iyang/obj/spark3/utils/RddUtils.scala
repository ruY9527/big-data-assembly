package com.iyang.obj.spark3.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object RddUtils {

  def createRddData(sc:SparkContext): RDD[Int] = {

    val iniRdd:RDD[Int] = sc.makeRDD(1 to 100, 3)
    iniRdd
  }

  def createStringRdd(sc:SparkContext): RDD[String] = {

    val strRdd:RDD[String] = sc.makeRDD(List("hello", "Spark", "Java", "Scala", "Flink", "Python"))
    strRdd
  }

  def createRandomRdd(sc:SparkContext): RDD[Int] = {

    val rdd: RDD[Int] = sc.makeRDD(List(2, 1, 3, 4, 6, 5))
    rdd
  }

}

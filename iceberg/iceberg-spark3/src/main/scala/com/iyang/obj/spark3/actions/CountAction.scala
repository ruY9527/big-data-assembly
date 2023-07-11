package com.iyang.obj.spark3.actions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object CountAction {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("CountAction").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val rdd:RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val count:Long = rdd.count()
    println(count)

    val firstNum:Int = rdd.first()
    println(firstNum)
    
    sc.stop()
  }

}

package com.iyang.obj.spark3.actions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
class AccumulatorBase {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("AccumulatorBase").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))
    // val rdd1:RDD[(String,Int)] = dataRDD.reduceByKey(_ + _)

    var sum = 0
    dataRDD.foreach{
      case (a,count) => {
        sum += count
        println("sum = " + sum)
      }
    }

    println(("a",sum))
    sc.stop()
  }

}

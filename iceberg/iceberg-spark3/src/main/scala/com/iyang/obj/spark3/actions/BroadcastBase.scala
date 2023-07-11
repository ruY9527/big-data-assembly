package com.iyang.obj.spark3.actions

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object BroadcastBase {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("BroadcastBase").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val lineRdd:RDD[String] = sc.makeRDD(List("WARN:Class Not Find", "INFO:Class Not Find", "DEBUG:Class Not Find"), 4)
    val str:String = "WARN"

    val bdStr: Broadcast[String] = sc.broadcast(str)

    val filterRdd:RDD[String] = lineRdd.filter {
      log => log.contains(bdStr.value)
    }

    filterRdd.foreach(println)
    sc.stop()
  }

}

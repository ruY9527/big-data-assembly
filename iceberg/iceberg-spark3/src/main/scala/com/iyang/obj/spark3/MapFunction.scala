package com.iyang.obj.spark3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object MapFunction {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("sparkMapFunction").setMaster("local[*]")

    val sc:SparkContext = new SparkContext(conf)
    val intRdd:RDD[Int] = sc.makeRDD(1 to 4, 2)

    // val mapRdd:RDD[Int] = intRdd.map(_ * 2)
    val mapRdd:RDD[Int] = intRdd.map(v => {
      println("调用次数")
      v * 2
    })

    mapRdd.collect().foreach(println)
    sc.stop()
  }

}

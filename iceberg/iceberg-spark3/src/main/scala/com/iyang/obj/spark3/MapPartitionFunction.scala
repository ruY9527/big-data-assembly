package com.iyang.obj.spark3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object MapPartitionFunction {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("SparkPartition").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val intRdd:RDD[Int] = sc.makeRDD(1 to 10, 5)
    // val iniRdd2:RDD[Int] = intRdd.mapPartitions(v => v.map(_ * 2))
    val iniRdd2:RDD[Int] = intRdd.mapPartitions(list => {
      println("调用xxxx")
      list.map(_ * 2)
    })

    iniRdd2.collect().foreach(println)

    sc.stop()
  }

}

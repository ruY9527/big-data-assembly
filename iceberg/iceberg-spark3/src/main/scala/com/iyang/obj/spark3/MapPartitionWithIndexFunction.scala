package com.iyang.obj.spark3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object MapPartitionWithIndexFunction {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PartitionWithIndex").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd:RDD[Int] = sc.makeRDD(1 to 100, 3)

    val indexRdd:RDD[(Int,Int)] = rdd.mapPartitionsWithIndex((index, items) => {
      items.map((index, _))
    })

    indexRdd.collect().foreach(println)
    sc.stop()
  }

}

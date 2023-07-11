package com.iyang.obj.spark3

import com.iyang.obj.spark3.utils.ListUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object FlatMapFunction {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("flatMapFunction").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val listRdd:RDD[List[Int]] = sc.makeRDD(ListUtils.createList(), 3)
    listRdd.flatMap(list => list).collect().foreach(println)

    sc.stop()
  }

}

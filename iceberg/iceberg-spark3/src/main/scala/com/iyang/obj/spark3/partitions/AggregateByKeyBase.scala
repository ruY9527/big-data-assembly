package com.iyang.obj.spark3.partitions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object AggregateByKeyBase {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("AggregateByKeyBase").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val listRdd:RDD[(String,Int)] = sc.makeRDD(List(("a", 1), ("a", 3), ("a", 5), ("b", 7), ("b", 2), ("b", 4), ("b", 6), ("a", 7)), 2)
    listRdd.aggregateByKey(0)(math.max(_,_),_+_).collect().foreach(println)

    sc.stop()
  }

}

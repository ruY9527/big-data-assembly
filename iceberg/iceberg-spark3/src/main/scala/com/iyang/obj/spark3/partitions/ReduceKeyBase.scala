package com.iyang.obj.spark3.partitions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc:
 * reduceByKey：按照key进行聚合，在shuffle之前有combine（预聚合）操作，返回结果是RDD[K,V]
 * groupByKey：按照key进行分组，直接进行shuffle
 * 开发指导：在不影响业务逻辑的前提下，优先选用reduceByKey。求和操作不影响业务逻辑，求平均值影响业务逻辑，后续会学习功能更加强大的归约算子，能够在预聚合的情况下实现求平均值
 *
 * * */
object ReduceKeyBase {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("ReduceKeyBase").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val listRdd:RDD[(String,Int)] = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)))
    val reduceRdd:RDD[(String,Int)] = listRdd.reduceByKey((v1, v2) => v1 + v2)

    reduceRdd.collect().foreach(println)
    sc.stop()
  }

}

package com.iyang.obj.spark3.cases

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object Top3Info {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("Top3Info").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val lineRdd:RDD[String] = sc.textFile("111.log")

    // 过滤需要的数据
    val tupleRdd:RDD[(String,String)] = lineRdd.map(line => {
      val data: Array[String] = line.split(" ")
      (data(1), data(4))
    })

    // 对省份加广告进行wordCount 统计
    val provinceCountRdd: RDD[((String,String),Int)] = tupleRdd.map((_, 1)).reduceByKey(_ + _)

    val tupleRDD1:RDD[((String,String), Int)] = lineRdd.map(line => {
      val data: Array[String] = line.split(" ")
      ((data(1), data(4)), 1)
    })

    val provinceCountRDD1: RDD[((String, String), Int)] = tupleRDD1.reduceByKey(_ + _)

    val value: RDD[(String, (String, Int))] = provinceCountRDD1.map(tuple => (tuple._1._1, (tuple._1._2, tuple._2)))

    provinceCountRDD1.map({
      case ((province,id),count) => (province,(id,count))
    })

    val provinceRDD1: RDD[(String, Iterable[(String, Int)])] = value.groupByKey()
    val result: RDD[(String, List[(String, Int)])] = provinceRDD1.mapValues(it => {
      val list1: List[(String, Int)] = it.toList
      list1.sortWith(_._2 > _._2).take(3)
    })

    result.collect().foreach(println)

    sc.stop()
  }

}

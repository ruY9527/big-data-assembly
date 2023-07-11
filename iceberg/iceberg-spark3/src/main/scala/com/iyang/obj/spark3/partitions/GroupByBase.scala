package com.iyang.obj.spark3.partitions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object GroupByBase {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("GroupByBase").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val listRdd:RDD[(String,Int)] = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)))
    val group: RDD[(String, Iterable[Int])] = listRdd.groupByKey()

    group.collect().foreach(println)
    group.map(t => (t._1,t._2.sum)).collect().foreach(println)

    sc.stop()
  }

}

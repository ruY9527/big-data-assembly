package com.iyang.obj.spark3.partitions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object SortByKeyBase {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("SortByKeyBase").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val listRdd:RDD[(Int,String)] = sc.makeRDD(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))
    listRdd.sortByKey(true).collect().foreach(println)

    // listRdd.sortByKey(false).collect().foreach(println)
    sc.stop()
  }

}

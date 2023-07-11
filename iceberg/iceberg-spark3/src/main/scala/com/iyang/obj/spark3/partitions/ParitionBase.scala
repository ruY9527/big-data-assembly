package com.iyang.obj.spark3.partitions

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object ParitionBase {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("PartitionBase").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val rdd:RDD[(Int,String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)
    val rdd2:RDD[(Int,String)] = rdd.partitionBy(new HashPartitioner(2))

    val indexRdd = rdd2.mapPartitionsWithIndex((index, data) => data.map((index, _)))
    indexRdd.collect().foreach(println)

    sc.stop()
  }

}

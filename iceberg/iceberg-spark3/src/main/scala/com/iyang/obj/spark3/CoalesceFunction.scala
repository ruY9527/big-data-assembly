package com.iyang.obj.spark3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc:
 * coalesce重新分区,可以选择是否进行shuffle过程;由参数 shuffle:Boolean = false/true决定
 * repartition实际上调用的是coalesce,进行shuffle
 * coalesce一般为缩减分区，如果扩大分区，不使用shuffle是没有意义的，repartition扩大分区执行shuffle
 * * */
object CoalesceFunction {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CoalesceFunction").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd:RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 8, 9, 7), 3)
    val coalesceRDD: RDD[Int] = rdd.coalesce(2)

    val indexRDD: RDD[(Int, Int)]  = coalesceRDD.mapPartitionsWithIndex((index, data) => {
      data.map((index, _))
    })

    indexRDD.collect().foreach(println)

    Thread.sleep(100000000)
    sc.stop()
  }

}

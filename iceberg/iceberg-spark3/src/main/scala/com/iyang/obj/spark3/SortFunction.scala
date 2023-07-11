package com.iyang.obj.spark3

import com.iyang.obj.spark3.utils.RddUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object SortFunction {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("SortFunction").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val intRdd: RDD[Int] = RddUtils.createRandomRdd(sc)
    val sortRdd:RDD[Int] = intRdd.sortBy(num => num)
    sortRdd.collect().foreach(println)

    val rddList:RDD[(Int,Int)] = sc.makeRDD(List((2, 1), (1, 2), (1, 1), (2, 2)))
    rddList.sortBy(t => t).collect().foreach(println)

    sc.stop()
  }

}

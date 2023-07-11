package com.iyang.obj.spark3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object PartitionConf {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val intRDD:RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 2)

    intRDD.foreach(println)
    // intRDD.saveAsTextFile("1");
    sparkContext.stop()

    // 具体的分区个数需要经过公式计算
    // 首先获取文件的总长度  totalSize
    // 计算平均长度  goalSize = totalSize / numSplits
    // 获取块大小 128M
    // 计算切分大小  splitSize = Math.max(minSize, Math.min(goalSize, blockSize));
    // 最后使用splitSize  按照1.1倍原则切分整个文件   得到几个分区就是几个分区
    // 实际开发中   只需要看文件总大小 / 填写的分区数  和块大小比较  谁小拿谁进行切分

  }

}

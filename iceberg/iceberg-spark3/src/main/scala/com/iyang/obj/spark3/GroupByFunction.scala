package com.iyang.obj.spark3

import com.iyang.obj.spark3.utils.RddUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc:
 *  groupBy存在shuffle过程
 *  shuffle将不同的分区数据进行打乱重组的过程
 *  shuffle一定回罗盘,可以在local模式下执行,通过4040查看效果
 *
 * * */
object GroupByFunction {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("GroupByFunction").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val intRdd:RDD[Int] = RddUtils.createRddData(sc)
    intRdd.groupBy(_%3).collect().foreach(println)

    val strRdd:RDD[String] = RddUtils.createStringRdd(sc)
    strRdd.groupBy(s => s.substring(0,1)).collect().foreach(println)

    sc.stop()

  }

}

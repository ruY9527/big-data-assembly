package com.iyang.obj.sql3

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object MyUDAF {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("MyUDAF").setMaster("local[*]")
    // val sc:SparkContext = new SparkContext(conf)
    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val df:DataFrame = spark.read.json("/home/luohong/coding/java/github_self/big-data-assembly/iceberg/iceberg-spark3/src/main/inputs/user.json")

    df.createTempView("user");
    spark.udf.register("myAvg", functions.udaf(new MyAvgUDAF))

    spark.sql("select myAvg(age) from user").show()
    spark.stop()
  }

}

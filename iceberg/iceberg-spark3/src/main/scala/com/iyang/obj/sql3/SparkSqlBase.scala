package com.iyang.obj.sql3

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object SparkSqlBase {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("SparkSqlBase").setMaster("local[*]")
    // val sc:SparkConf = new SparkContext(conf)
    val session:SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df:DataFrame = session.read.json("/home/luohong/coding/java/github_self/big-data-assembly/iceberg/iceberg-spark3/src/main/inputs/user.json")
    df.show()
    session.stop()

  }

}

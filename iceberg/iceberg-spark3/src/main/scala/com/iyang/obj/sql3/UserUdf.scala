package com.iyang.obj.sql3

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object UserUdf {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserUdf")
    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df:DataFrame = spark.read.json("/home/luohong/coding/java/github_self/big-data-assembly/iceberg/iceberg-spark3/src/main/inputs/user.json")

    df.createTempView("user");
    spark.udf.register("addName", (x:String) => "Name: " +x)

    spark.sql("select addName(name),age from user").show()

    spark.stop()
  }

}

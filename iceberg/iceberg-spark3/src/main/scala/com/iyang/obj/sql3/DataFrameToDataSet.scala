package com.iyang.obj.sql3

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession,Dataset}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object DataFrameToDataSet {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("DataFrameToDataSet")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df:DataFrame = spark.read.json("/home/luohong/coding/java/github_self/big-data-assembly/iceberg/iceberg-spark3/src/main/inputs/user.json")

    import spark.implicits._
    val userDataSet:Dataset[User] = df.as[User]
    userDataSet.show()

    val userDataFrame:DataFrame = userDataSet.toDF()
    userDataFrame.show()
    spark.stop()
  }

}

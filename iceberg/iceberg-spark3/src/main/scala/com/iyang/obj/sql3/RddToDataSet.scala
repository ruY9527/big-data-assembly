package com.iyang.obj.sql3

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object RddToDataSet {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("RddToDataSet").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    val lineRdd:RDD[String] = sc.textFile("/home/luohong/coding/java/github_self/big-data-assembly/iceberg/iceberg-spark3/src/main/inputs/user.txt")

    val rdd: RDD[(String, Long)] = lineRdd.map {
      line => {
        val fields: Array[String] = line.split(",")
        (fields(0), fields(1).toLong)
      }
    }

    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val ds:Dataset[(String,Long)] = rdd.toDS()
    ds.show()

   //  val ds:DataSet[(String,Int)] = rdd.toDF()
    sc.stop()
  }

}

package com.iyang.obj.sql3

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object RddToDataFrame {


  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("RddToDataFrame").setMaster("local[*]")
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

    val df:DataFrame = rdd.toDF("name", "age")
    df.show()

    val userRdd:RDD[User] = rdd.map {
      t => {
        new User(t._1, t._2)
      }
    }

    // val value = userRdd.toDF()


    sc.stop()

  }

}

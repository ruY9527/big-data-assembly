package com.iyang.obj.sql3

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object SparkMySql {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("SparkMySql").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df1: DataFrame = spark.read.json("/home/luohong/coding/java/github_self/big-data-assembly/iceberg/iceberg-spark3/src/main/inputs/mysqlUser.json")


    import spark.implicits._
    val userDataSet: Dataset[MySqlUser] = df1.as[MySqlUser]

   // userDataSet
    userDataSet.write.format("jdbc")
      .option("url", "jdbc:mysql://172.21.129.214:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "YdMysqlxx9")
      .option("dbtable", "t_users")
      .mode(SaveMode.Append)
      .save()


/*    val df:DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://172.21.129.214:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "YdMysqlxx9")
      .option("dbtable", "t_users")
      .load()

    df.createTempView("user")
    spark.sql("select user,url from user").show()*/

    spark.stop()
  }

  case class MySqlUser(user:String,url:String)
}

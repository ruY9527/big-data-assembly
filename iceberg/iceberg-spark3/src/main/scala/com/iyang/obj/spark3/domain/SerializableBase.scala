package com.iyang.obj.spark3.domain

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object SerializableBase {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("SerializableBase").setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[User]))
    val sc:SparkContext = new SparkContext(conf)

    val user1 = new User()
    user1.name = "ZhangSan"

    val user2 = new User()
    user2.name = "lisi"

    val userRdd:RDD[User] = sc.makeRDD(List(user1, user2))
    userRdd.foreach(user => println(user.name))

    sc.stop()
  }

}

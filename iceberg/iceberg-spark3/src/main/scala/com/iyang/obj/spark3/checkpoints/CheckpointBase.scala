package com.iyang.obj.spark3.checkpoints

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object CheckpointBase {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("CheckpointBase").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)

    sc.setCheckpointDir("./checkpoint111")
    val lineRdd:RDD[String] = sc.textFile("input1")

    val wordRdd:RDD[String] = lineRdd.flatMap(line => line.split(" "))
    val wordToOneRdd: RDD[(String, Long)] = wordRdd.map {
      word => {
        (word, System.currentTimeMillis())
      }
    }

    wordToOneRdd.checkpoint()
    wordToOneRdd.collect().foreach(println)

    wordToOneRdd.collect().foreach(println)
    wordToOneRdd.collect().foreach(println)

    Thread.sleep(100000)
    sc.stop()
  }

}

package com.iyang.obj.streams

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object SparkStreamQueue {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("SparkStreamQueue").setMaster("local[*]")
    val sc = new StreamingContext(conf, Seconds(4))

    val rddQueue = new mutable.Queue[RDD[Int]]()

    val inputDsStream = sc.queueStream(rddQueue, oneAtATime = false)
    val sumDsStream = inputDsStream.reduce(_ + _)
    sumDsStream.print()

    sc.start()

    for(i <- 1 to 5) {
      rddQueue += sc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(2000)
    }

    sc.awaitTermination()
  }

}

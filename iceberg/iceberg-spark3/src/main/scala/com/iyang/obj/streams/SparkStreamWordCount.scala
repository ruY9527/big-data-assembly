package com.iyang.obj.streams

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object SparkStreamWordCount {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("SparkStreamWordCount").setMaster("local[*]")
    val sc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val lineDsStream = sc.socketTextStream("127.0.0.1", 9998)
    val wordDStream = lineDsStream.flatMap(_.split(" "))

    val wordToOneDStream = wordDStream.map((_, 1))

    val wordToSumDStream = wordToOneDStream.reduceByKey(_ + _)

    wordToSumDStream.print()

    sc.start()
    sc.awaitTermination()
  }

}

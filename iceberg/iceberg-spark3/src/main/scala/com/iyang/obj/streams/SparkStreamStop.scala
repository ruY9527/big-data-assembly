package com.iyang.obj.streams

import com.iyang.obj.streams.domain.MonitorStop
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
object SparkStreamStop {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("sparkStreaming")
    sparkconf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ssc: StreamingContext = new StreamingContext(sparkconf, Seconds(3))
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("172.0.0.1", 9998)

    lineDStream.flatMap(_.split(" "))
      .map((_,1))
      .print()

    new Thread(new MonitorStop(ssc)).start()

    ssc.stop()
    ssc.awaitTermination()
  }

}

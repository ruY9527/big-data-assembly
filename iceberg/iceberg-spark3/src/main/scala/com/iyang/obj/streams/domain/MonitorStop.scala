package com.iyang.obj.streams.domain

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}

import java.net.URI

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
 case class MonitorStop(scc:StreamingContext) extends Runnable {

  override def run(): Unit = {

    val fs: FileSystem = FileSystem.get(
      new URI("hdfs://udp215:8020/tmp/checkpoint"),
      new Configuration(),
      "hadoop"
    )

    while (true) {
      Thread.sleep(5000)
      val result: Boolean = fs.exists(new Path("hdfs://udp215:8020/tmp/stopSpark"))
      if (result) {
        val state:StreamingContextState = scc.getState()
        if (state == StreamingContextState.ACTIVE) {
          scc.stop(stopSparkContext = true, stopGracefully = true)
          System.exit(0)
        }
      }
    }

  }

}

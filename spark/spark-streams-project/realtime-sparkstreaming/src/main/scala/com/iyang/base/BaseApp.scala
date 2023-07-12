package com.iyang.base

import org.apache.spark.streaming.StreamingContext

/** **
 * author: BaoYang
 * date: 2023/7/12
 * desc: 
 * * */

abstract class BaseApp {

  var groupName:String

  var batchDuration:Int

  var appName:String

  var topic:String

  var context:StreamingContext = null

  def runApp(code: => Unit): Unit = {

    try {
      code
      context.start()
      context.awaitTermination()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw new RuntimeException("运行错误")
    }

  }

}

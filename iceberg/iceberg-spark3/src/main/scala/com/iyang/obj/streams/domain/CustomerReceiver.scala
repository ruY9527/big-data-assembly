package com.iyang.obj.streams.domain

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
class CustomerReceiver (host:String, port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY)  {

  override def onStart(): Unit = {
    new Thread("socket receiver") {
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  def receive():Unit = {

    val socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(
      socket.getInputStream, StandardCharsets.UTF_8
    ))

    var input:String = reader.readLine()
    while (!isStopped() && input != null) {
      store(input)
      input = reader.readLine()
    }

    reader.close()
    socket.close()
    restart("restart")
  }


  override def onStop(): Unit = {}

}

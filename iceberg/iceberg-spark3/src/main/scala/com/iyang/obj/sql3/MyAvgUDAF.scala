package com.iyang.obj.sql3

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator


/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
class MyAvgUDAF extends Aggregator[Long,Buff,Double] {
  override def zero: Buff = Buff(0L,0L)

  override def reduce(b: Buff, a: Long): Buff = {
    b.sum = b.sum + a
    b.count = b.count + 1
    b
  }

  override def merge(b1: Buff, b2: Buff): Buff = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  override def finish(reduction: Buff): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

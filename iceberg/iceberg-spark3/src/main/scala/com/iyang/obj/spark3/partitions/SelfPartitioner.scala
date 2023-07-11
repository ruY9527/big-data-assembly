package com.iyang.obj.spark3.partitions

import org.apache.spark.Partitioner

/** **
 * author: BaoYang
 * date: 2023/7/11
 * desc: 
 * * */
class SelfPartitioner(num:Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    key match {
      case s:String => 0
      case i:Int => i % numPartitions
      case _ => 0
    }
    1
  }
}

package com.iyang.gmv

import com.iyang.base.BaseApp
import com.iyang.jdbc.utils.JDBCUtil
import com.iyang.utils.TopicConstant
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.collection.mutable

/** **
 * author: BaoYang
 * date: 2023/7/12
 * desc: 
 * * */


object GMVApp extends BaseApp {

  override var groupName: String = "baoyang2"
  override var batchDuration: Int = 10
  override var appName: String = "GMVApp"
  override var topic: String = ???


  def readHistoryOffsets(groupId:String, topicName:String): Unit = {

    val result = new mutable.HashMap[TopicPartition, Long]()

    val sql =
      """
        |select
        |   partitionId,untilOffset
        |from offsets
        |where groupId=? and topic=?
        |
        |
        |""".stripMargin

    var connection:Connection = null
    var ps: PreparedStatement = null

    try {
      connection = JDBCUtil.getConnection()
      ps = connection.prepareStatement(sql)

      val resultSet: ResultSet = ps.executeQuery()
      while (resultSet.next()) {
        result.put(
          new TopicPartition(topicName,
          resultSet.getInt("partitionId")),
          resultSet.getLong("untilOffset"))
      }
    } catch {
      case e:Exception =>
        e.printStackTrace()
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (connection != null) {
        connection.close()
      }
    }
  }

  def writeDataAndOffsetsInCommontransaction(result:Array[((String,String), Double)],
                                             ranges:Array[OffsetRange]): Unit = {

    var sql1 =
      """
        |
        |INSERT INTO gmvstats
        |VALUES(?,?,?)
        |ON DUPLICATE KEY UPDATE gmv=values(gmv) + gmv
        |
        |
        |""".stripMargin

    var sql2 =
      """
        |
        |replace INTO offsets VALUES(?,?,?,?)
        |
        |
        |
        |""".stripMargin

    var connection:Connection = null
    var ps1:PreparedStatement = null
    var ps2:PreparedStatement = null

    try {
      connection = JDBCUtil.getConnection()

      ps1 = connection.prepareStatement(sql1)
      ps2 = connection.prepareStatement(sql2)

      for(((date,hour),gmv) <- result){
        ps1.setString(1,date)
        ps1.setString(2,hour)
        ps1.setDouble(3,gmv)

        ps1.addBatch()
      }

      for(offsetRange <- ranges){
        ps2.setString(1,groupName)
        ps2.setString(2, TopicConstant.ORDER_INFO)
        ps2.setInt(3, offsetRange.partition)
        ps2.setLong(4, offsetRange.untilOffset)

        ps2.addBatch()
      }

      val dataRepsonse: Array[Int]  = ps1.executeBatch()
      val offsetsRepsonse: Array[Int] = ps2.executeBatch()

      connection.commit()

      println("数据写成功了:" + dataRepsonse.size)
      println("offsets写成功了:" + offsetsRepsonse.size)
    } catch {
      case e:Exception =>
        connection.rollback()
        e.printStackTrace()
    } finally {
      if (ps1 != null) {
        ps1.close()
      }
      if (connection != null) {
        connection.close()
      }
    }

  }

}

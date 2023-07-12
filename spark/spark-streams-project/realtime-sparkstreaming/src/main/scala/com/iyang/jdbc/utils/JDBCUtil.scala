package com.iyang.jdbc.utils

import java.sql.Connection
import java.util.Properties
import com.alibaba.druid.pool.DruidDataSourceFactory
import com.iyang.utils.PropertiesUtil

import javax.sql.DataSource


/** **
 * author: BaoYang
 * date: 2023/7/12
 * desc: 
 * * */
object JDBCUtil {

  // 创建连接池对象
  var dataSource: DataSource = init()

  def init(): DataSource = {

    val paramMap = new java.util.HashMap[String, String]()

    // val properties: Properties = PropertiesUtil.load("config.properties")

    paramMap.put("driverClassName", PropertiesUtil.getProperty("jdbc.driver.name"))
    paramMap.put("url", PropertiesUtil.getProperty("jdbc.url"))
    paramMap.put("username", PropertiesUtil.getProperty("jdbc.user"))
    paramMap.put("password", PropertiesUtil.getProperty("jdbc.password"))
    paramMap.put("maxActive", PropertiesUtil.getProperty("jdbc.datasource.size"))

    // 使用Druid连接池对象
    DruidDataSourceFactory.createDataSource(paramMap)
  }

  // 从连接池中获取连接对象
  def getConnection(): Connection = {
    dataSource.getConnection
  }

}

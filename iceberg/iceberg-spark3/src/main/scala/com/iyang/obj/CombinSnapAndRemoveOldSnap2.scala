package com.iyang.obj

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog

/** *
 * big-data-assembly
 * com.iyang.obj
 *
 * @author: 鲍洋
 * @data: 2022/11/29
 * @desc:
 * **/
object CombinSnapAndRemoveOldSnap2 {

  def main(args: Array[String]): Unit = {

    val conf = new Configuration()
    val catalog = new HadoopCatalog(conf,"hdfs://mycluster/sparkoperateiceberg")
    val table: Table = catalog.loadTable(TableIdentifier.of("mydb","mytest"))

/*    Actions.forTable(table).rewriteDataFiles().targetSizeInBytes(1024)//1kb，指定生成合并之后文件大小
      .execute()*/

  }

}

package com.iyang.obj

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.actions.SparkActions

/** *
 * big-data-assembly
 * com.iyang.obj
 *
 * @author: 鲍洋
 * @data: 2022/11/29
 * @desc:
 * **/
object CombinSnapAndRemoveOldSnap {


  def main(args: Array[String]): Unit = {

    val conf = new Configuration()
    val catalog = new HadoopCatalog(conf, "")

    val table = catalog.loadTable(TableIdentifier.of("ods_bigscreen", "o_getAllApps"))

    SparkActions.get().rewriteDataFiles(table).execute();
    // Actions.forTable(table).rewriteDataFiles().execute()
    table.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit()
  }

}

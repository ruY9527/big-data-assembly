package com.iyang.flinks;

import com.iyang.flinks.datas.MyOrder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

/***
 * @author: baoyang
 * @data: 2022/8/7
 * @desc:
 ***/
public class TableBatchDemo {

    public static void main(String[] args) {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(executionEnvironment);

        /*batchTableEnvironment.fromDataSet(new DataSet<Object>() {

        });*/

    }

}

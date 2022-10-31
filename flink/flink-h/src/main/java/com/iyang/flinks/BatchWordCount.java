package com.iyang.flinks;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;


/***
 * @author: baoyang
 * @data: 2022/8/7
 * @desc: 批处理
 * flink1.12开始之后, 也是可以通过 DataStreamAPI 来进行设置 batch 来进行批处理
 *
 ***/
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        // 环境创建
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // 批的模式执行.
        // 读取文件,使用有的文件来进行模拟
        DataSource<String> dataSource = executionEnvironment.fromElements("Flink batch demo", "batch demo", "demo");

        // 数据格式的切割和进行统计
        DataSet<Tuple2<String, Integer>> sum = dataSource.flatMap(new LineSplitter()).groupBy(0).sum(1);
        // 打印
        sum.print();
    }

}

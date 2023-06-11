package com.iyang.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/****
 * author: BaoYang
 * date: 2023/6/11
 * desc:
 ***/
public class BatchWordCount {


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> textDs = env.readTextFile("/home/luohong/coding/java/github_self/big-data-assembly/flink17/flink17-base/files/words.txt");

        FlatMapOperator<String, Tuple2<String, Long>> mapOperator = textDs.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {

                String[] words = s.split(" ");
                Arrays.stream(words).forEach(ss -> collector.collect(Tuple2.of(ss, 1L)));
            }
        });

        // word 进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG  = mapOperator.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

        sum.print();

    }


}

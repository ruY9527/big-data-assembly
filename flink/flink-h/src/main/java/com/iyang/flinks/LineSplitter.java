package com.iyang.flinks;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/***
 * @author: baoyang
 * @data: 2022/8/7
 * @desc:
 ***/
public class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>> {

    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

        String[] strArr = s.split(" ");
        for (String s1 : strArr) {
            collector.collect(new Tuple2<>(s1,1));
        }
        
    }

}

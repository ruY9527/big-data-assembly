package com.iyang.base.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class TupleUtils {


    public static List<Tuple2<String, Integer>> createTuple2Info(){

        List<Tuple2<String, Integer>> list = new ArrayList<>(
                Arrays.asList(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 2),
                        Tuple2.of("b", 3),
                        Tuple2.of("c", 4)
                )
        );

        return list;
    }


    public static List<Tuple3<String,Integer,Integer>> createTuple3Info(){

        List<Tuple3<String,Integer,Integer>> list = new ArrayList<>(
                Arrays.asList(
                        Tuple3.of("a", 1, 1),
                        Tuple3.of("a", 11, 1),
                        Tuple3.of("b", 2, 1),
                        Tuple3.of("b", 12, 1),
                        Tuple3.of("c", 14, 1),
                        Tuple3.of("d", 15, 1)
                )
        );

        return list;
    }


}

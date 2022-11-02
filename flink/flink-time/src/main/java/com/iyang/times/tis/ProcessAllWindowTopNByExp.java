package com.iyang.times.tis;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: baoyang
 * @data: 2022/11/2
 * @desc:
 ***/
public class ProcessAllWindowTopNByExp {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


    }

}

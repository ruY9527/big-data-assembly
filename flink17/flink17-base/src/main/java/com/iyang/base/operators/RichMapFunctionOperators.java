package com.iyang.base.operators;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class RichMapFunctionOperators {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.fromElements(1,2,3,4).map(new UserRichMapFunction()).print();
        env.execute();

    }

}

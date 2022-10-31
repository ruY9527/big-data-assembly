package com.iyang.flinks;

import com.iyang.flinks.sources.IntCustomSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: baoyang
 * @data: 2022/10/31
 * @desc:
 ***/
public class ParallelSourceExample {

    public static void main(String[] args) throws Exception {

        // 这里是没有停止的操作
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new IntCustomSource()).setParallelism(2).print();
        env.execute("parallel execution....");

    }

}

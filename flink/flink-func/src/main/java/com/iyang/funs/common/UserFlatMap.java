package com.iyang.funs.common;

import com.iyang.funs.datas.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class UserFlatMap implements FlatMapFunction<Event , String> {

    // 返回参数,取决于所传入参数的具体逻辑,可以与原数据流相同,也可以不同,用户也可以重写这个方法
    // 通过 collector 收集器来对数据进行收集
    @Override
    public void flatMap(Event value, Collector<String> out) throws Exception {

        boolean equals = "Mary".equals(value.user);
        if(equals){
            out.collect(value.user);
        } else if ("Bob".equalsIgnoreCase(value.user)) {
            out.collect(value.user);
            out.collect(value.url);
        }

    }

}

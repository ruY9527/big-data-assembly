package com.iyang.base.operators;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class UserRichMapFunction extends RichMapFunction<Integer, Integer> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("索引是：" + getRuntimeContext().getIndexOfThisSubtask() + " 的任务的生命周期开始");
    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("索引是：" + getRuntimeContext().getIndexOfThisSubtask() + " 的任务的生命周期结束");
    }

    @Override
    public Integer map(Integer value) throws Exception {
        return value + 1;
    }
}

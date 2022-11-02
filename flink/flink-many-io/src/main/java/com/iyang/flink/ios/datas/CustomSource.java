package com.iyang.flink.ios.datas;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/***
 * @author: baoyang
 * @data: 2022/11/2
 * @desc:
 ***/
public class CustomSource implements SourceFunction<Event> {
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {

        // 直接发出测试数据
        ctx.collect(new Event("Mary", "./home", 1000L));
        // 为了更加明显，中间停顿 5 秒钟
        Thread.sleep(5000L);
        // 发出 10 秒后的数据
        ctx.collect(new Event("Mary", "./home", 11000L));
        Thread.sleep(5000L);
        // 发出 10 秒+1ms 后的数据
        ctx.collect(new Event("Alice", "./cart", 11001L));
        Thread.sleep(5000L);

    }

    @Override
    public void cancel() {

    }
}

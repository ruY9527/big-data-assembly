package com.iyang.times.triggers;

import com.iyang.times.datas.Event;
import com.iyang.times.datas.UrlViewCount;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class WindowResult extends ProcessWindowFunction<Event, UrlViewCount, String, TimeWindow> {


    @Override
    public void process(String s, Context context,
                        Iterable<Event> elements, Collector<UrlViewCount> out) throws Exception {

        // 对结果进行一个收集
        out.collect(new UrlViewCount(
                s,
                // 获取迭代器的个数
                elements.spliterator().getExactSizeIfKnown(),
                context.window().getStart(),
                context.window().getEnd()));

    }

}

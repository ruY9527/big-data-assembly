package com.iyang.times.funcs;

import com.iyang.times.datas.UrlViewCount;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

    @Override
    public void process(String s, Context context,
                        Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {

        long start = context.window().getStart();
        long end = context.window().getEnd();
        out.collect(new UrlViewCount(s, elements.iterator().next(), start, end));

    }

}

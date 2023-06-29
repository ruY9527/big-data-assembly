package com.iyang.base.windows;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class SelfProcessWindowFunction extends ProcessWindowFunction<String, String, String, TimeWindow> {

    @Override
    public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {

        long count = elements.spliterator().estimateSize();
        long winStart = context.window().getStart();
        long winEnd = context.window().getEnd();

        String windowStart = DateFormatUtils.format(winStart, "yyyy-MM-dd HH:mm:ss.SSS");
        String windowEnd = DateFormatUtils.format(winEnd, "yyyy-MM-dd HH:mm:ss.SSS");

        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据; ====> " + elements.toString());


    }

}

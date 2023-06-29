package com.iyang.base.utils;

import com.iyang.base.pojos.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class ProcessWindowFunctionUtils {

    public static ProcessWindowFunction<WaterSensor, String, String, TimeWindow> createWaterSensorFunction(){

        ProcessWindowFunction<WaterSensor, String, String, TimeWindow> processWindowFunction = new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                long startTs = context.window().getStart();
                long endTs = context.window().getEnd();
                String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                long count = elements.spliterator().estimateSize();

                out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
            }
        };

        return processWindowFunction;
    }


}

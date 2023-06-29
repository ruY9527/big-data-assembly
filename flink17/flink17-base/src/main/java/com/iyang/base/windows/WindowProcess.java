package com.iyang.base.windows;

import com.iyang.base.functions.WaterSensorMapFunction;
import com.iyang.base.pojos.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class WindowProcess {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDs = env.socketTextStream("localhost", 8899).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKs = sensorDs.keyBy(s -> s.getId());

        WindowedStream<WaterSensor, String, TimeWindow> sensorWin = sensorKs.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<String> process = sensorWin.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {

                long count = elements.spliterator().estimateSize();
                long winStart = context.window().getStart();
                long winEnd = context.window().getEnd();

                String windowStart = DateFormatUtils.format(winStart, "yyyy-MM-dd HH:mm:ss.SSS");
                String windowEnd = DateFormatUtils.format(winEnd, "yyyy-MM-dd HH:mm:ss.SSS");

                out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据; ====> " + elements.toString());
            }
        });

        process.print();
        env.execute();

    }

}

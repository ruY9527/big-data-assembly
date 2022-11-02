package com.iyang.times.tis;

import com.iyang.times.datas.ClickSource;
import com.iyang.times.datas.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/***
 * @author: baoyang
 * @data: 2022/11/2
 * @desc:
 ***/
public class ProcessingTimeTimerByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> source = env.addSource(new ClickSource(), "YangSource");
        source.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, Object>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<Object> out) throws Exception {
                        long currTs = ctx.timerService().currentProcessingTime();
                        out.collect("数据到达, 到达时间: " + new Timestamp(currTs));
                        ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                        out.collect("定时器触发,触发时间:" + new Timestamp(timestamp));
                    }
                }).print();
        env.execute("runing...");
    }

}

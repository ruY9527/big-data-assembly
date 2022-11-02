package com.iyang.times.tis;

import com.iyang.times.datas.CustomSource;
import com.iyang.times.datas.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/***
 * @author: baoyang
 * @data: 2022/11/2
 * @desc:
 ***/
public class EventTimeTimerByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        streamOperator.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, Object>() {

                    @Override
                    public void processElement(Event value, Context ctx, Collector<Object> out) throws Exception {
                        out.collect("数据到达时间: " + ctx.timestamp());
                        out.collect("数据到达,水位线为: " + ctx.timerService().currentWatermark() + "\n ---- 分割线------");
                        ctx.timerService().registerProcessingTimeTimer(ctx.timestamp() + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                        out.collect("定时器触发,触发时间: " + timestamp);
                    }
                }).print();

        env.execute("run...");

    }

}

package com.iyang.times.triggers;

import com.iyang.times.datas.ClickSource;
import com.iyang.times.datas.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class TriggerByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }))
        .keyBy(r -> r.url)
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .trigger(new MyTrigger())
        .process(new WindowResult())
        .print();

        env.execute("runing...");

    }

}

package com.iyang.times.wins;

import com.iyang.times.datas.ClickSource;
import com.iyang.times.datas.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc: 窗口的触发计算(Fire) 和 清除(Purge)
 ***/
public class WindowReduceByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 定义 Source
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));


        stream.map(new MapFunction<Event, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {

                // 将数据转化为二元组,方便计算
                return Tuple2.of(value.user, 1L);
            }

        })
                .keyBy(r -> r.f0)
                // 设置滚动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                      @Override
                      public Tuple2<String, Long> reduce(Tuple2<String, Long> value1,
                                                         Tuple2<String, Long> value2) throws Exception {

                          // 定义累加规则,窗口闭合时,向下游发送累加结果
                          return Tuple2.of(value1.f0 , value1.f1 + value2.f1);
                      }
                }).print();
        env.execute("running...");

    }

}

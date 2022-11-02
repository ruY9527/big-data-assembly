package com.iyang.times.wins;

import com.iyang.times.datas.Event;
import com.iyang.times.datas.UrlViewCount;
import com.iyang.times.funcs.UrlViewCountAgg;
import com.iyang.times.funcs.UrlViewCountResult;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/***
 * @author: baoyang
 * @data: 2022/11/2
 * @desc:
 ***/
public class ProcessLateDataByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> streamOperator = env.socketTextStream("127.0.0.1", 8899)
                .map(new MapFunction<String, Event>() {

                    @Override
                    public Event map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                    }

                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        OutputTag<Event> outputTag = new OutputTag<Event>("late"){};

        SingleOutputStreamOperator<UrlViewCount> aggregate = streamOperator.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        aggregate.print("result...");
        aggregate.getSideOutput(outputTag).print("late");

        streamOperator.print("input");
        env.execute("runing....");

    }

}

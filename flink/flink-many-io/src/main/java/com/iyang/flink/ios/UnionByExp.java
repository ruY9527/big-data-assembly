package com.iyang.flink.ios;

import com.iyang.flink.ios.datas.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/***
 * @author: baoyang
 * @data: 2022/11/2
 * @desc:
 ***/
public class UnionByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("bd211", 8899)
                .map(data -> {
                    String[] split = data.split(",");
                    return new Event(split[0].trim(), split[1].trim(), Long.parseLong(split[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream1.print("streams1");

        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("bd212", 8899)
                .map(data -> {
                    String[] split = data.split(",");
                    return new Event(split[0].trim(), split[1].trim(), Long.parseLong(split[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream2.print("stream2");

        stream1.union(stream2).process(new ProcessFunction<Event, Object>() {

            @Override
            public void processElement(Event value, Context ctx, Collector<Object> out) throws Exception {
                // 水位线
                out.collect("水位线: " + ctx.timerService().currentWatermark());

            }

        }).print();

        env.execute("runing....many io");

    }

}

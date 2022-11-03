package com.iyang.flink.ios;

import com.iyang.flink.ios.datas.ClickSource;
import com.iyang.flink.ios.datas.Event;
import com.iyang.flink.ios.datas.PeriodicPvResult;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/***
 * @author: baoyang
 * @data: 2022/11/3
 * @desc:
 ***/
public class PeriodicPvByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        streamOperator.print("input");
        streamOperator.keyBy(data -> data.user)
                .process(new PeriodicPvResult())
                .print();

        env.execute("run job...");
    }

}

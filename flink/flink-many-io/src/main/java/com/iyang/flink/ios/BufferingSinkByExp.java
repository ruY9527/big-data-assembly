package com.iyang.flink.ios;

import com.iyang.flink.ios.datas.BufferingSink;
import com.iyang.flink.ios.datas.ClickSource;
import com.iyang.flink.ios.datas.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: baoyang
 * @data: 2022/11/3
 * @desc:
 ***/
public class BufferingSinkByExp {

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
        streamOperator.addSink(new BufferingSink(10));
        env.execute("runing...");

    }

}

package com.iyang.flink.ios;

import com.iyang.flink.ios.datas.Event;
import com.iyang.flink.ios.func.OrderMatchResult;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: baoyang
 * @data: 2022/11/2
 * @desc:
 ***/
public class BillCheckByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 来自 app 的支付日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> streamOperator = env.fromElements(Tuple3.of("order-1", "app", 1000L), Tuple3.of("order-2", "app", 2000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        }));

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdPartStream = env.fromElements(Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-3", "third-party", "success", 4000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                                return element.f3;
                            }
                        }));

        streamOperator.connect(thirdPartStream)
                .keyBy(data -> data.f0 , data -> data.f0)
                .process(new OrderMatchResult())
                .print();

        env.execute("runing...");

    }

}

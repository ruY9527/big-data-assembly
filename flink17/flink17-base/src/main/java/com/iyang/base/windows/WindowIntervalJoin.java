package com.iyang.base.windows;

import com.iyang.base.utils.TupleUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class WindowIntervalJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromCollection(TupleUtils.createTuple2Info())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((v, t) -> v.f1 * 1000L)
                );

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.fromCollection(TupleUtils.createTuple3Info()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((v, t) -> v.f1 * 1000L)
        );

        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r1 -> r1.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r2 -> r2.f0);

        ks1.intervalJoin(ks2).between(Time.seconds(-2), Time.seconds(2))
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {

                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + "<------>" + right);
                    }
                }).print();

        env.execute();

    }

}

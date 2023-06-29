package com.iyang.base.windows;

import com.iyang.base.utils.TupleUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class WindowJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromCollection(TupleUtils.createTuple2Info()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((v, t) -> v.f1 * 1000L)
        );


        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.fromCollection(TupleUtils.createTuple3Info()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((v, t) -> v.f1 * 1000L)
        );

        DataStream<String> join = ds1.join(ds2).where(r1 -> r1.f0).equalTo(r2 -> r2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                        return first + "<------>" + second;
                    }
                });

        join.print();

        env.execute();

    }

}

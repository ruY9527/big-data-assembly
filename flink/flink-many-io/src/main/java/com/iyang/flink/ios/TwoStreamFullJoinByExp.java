package com.iyang.flink.ios;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/***
 * @author: baoyang
 * @data: 2022/11/3
 * @desc:
 ***/
public class TwoStreamFullJoinByExp {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> streamOperator1 = env.fromElements(
                Tuple3.of("a", "stream-1", 1000L),
                Tuple3.of("b", "stream-1", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> streamOperator2 = env.fromElements(
                Tuple3.of("a", "stream-2", 3000L),
                Tuple3.of("b", "stream-2", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));

        streamOperator1.keyBy(r -> r.f0)
                .connect(streamOperator2.keyBy(r -> r.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Object>() {

                    private ListState<Tuple3<String, String, Long>> stream1StateList;
                    private ListState<Tuple3<String, String, Long>> stream2StateList;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        stream1StateList = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream1-list",
                                Types.TUPLE(Types.STRING, Types.STRING)));

                        stream2StateList = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream1-list",
                                Types.TUPLE(Types.STRING, Types.STRING)));

                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> value,
                                                Context ctx, Collector<Object> out) throws Exception {

                        stream1StateList.add(value);
                        for (Tuple3<String, String, Long> tuple3 : stream2StateList.get()) {
                            out.collect(value + "=>" + tuple3);
                        }

                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> value,
                                                Context ctx, Collector<Object> out) throws Exception {
                        stream2StateList.add(value);
                        for (Tuple3<String, String, Long> tuple3 : stream1StateList.get()) {
                            out.collect(tuple3 + "=>" + value);
                        }
                    }
                }).print();

        env.execute("job....");

    }

}

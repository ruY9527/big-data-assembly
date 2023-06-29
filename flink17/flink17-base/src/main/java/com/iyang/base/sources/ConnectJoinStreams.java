package com.iyang.base.sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class ConnectJoinStreams {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(2, "a2"),
                Tuple2.of(3, "a3"),
                Tuple2.of(4, "a4")
        );

        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 1),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
                );

        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2);
        // 多并行度下，需要根据 关联条件 进行keyby，才能保证key相同的数据到一起去，才能匹配上
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectKey = connect.keyBy(s1 -> s1.f0, s2 -> s2.f0);

        SingleOutputStreamOperator<Object> result = connectKey.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, Object>() {

            Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();

            @Override
            public void processElement1(Tuple2<Integer, String> value,
                                        CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, Object>.Context ctx, Collector<Object> out) throws Exception {
                Integer id = value.f0;
                if (!s1Cache.containsKey(id)) {
                    List<Tuple2<Integer, String>> s1Values = new ArrayList<>();
                    s1Values.add(value);
                    s1Cache.put(id, s1Values);
                } else {
                    s1Cache.get(id).add(value);
                }
                if (s2Cache.containsKey(id)) {
                    s2Cache.get(id).forEach(vList -> {
                        out.collect("s1:" + value + "<-------->s2:" + vList);
                    });
                }

            }

            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value,
                                        CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, Object>.Context ctx, Collector<Object> out) throws Exception {
                Integer id = value.f0;
                if (!s2Cache.containsKey(id)) {
                    List<Tuple3<Integer, String, Integer>> s2Value = new ArrayList<>();
                    s2Value.add(value);
                    s2Cache.put(id, s2Value);
                } else {
                    s2Cache.get(id).add(value);
                }

                if (s1Cache.containsKey(id)) {
                    s1Cache.get(id).forEach(vList -> {
                        out.collect("s1:" + vList + "<------------->s2:" + value);
                    });
                }

            }
        });

        result.print();
        env.execute();

    }

}

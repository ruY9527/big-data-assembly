package com.iyang.funs.agg;

import com.iyang.funs.datas.Event;
import com.iyang.funs.sources.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class TransMapReduceByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 添加数据源进来
        env.addSource(new ClickSource()).map(new MapFunction<Event, Tuple2<String, Long>>() {

            // 将 Event 转化为 元组类型
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.user, 1L);
            }

        }).keyBy(r -> r.f0)  // 使用用户名进行分流
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1,
                                                       Tuple2<String, Long> value2) throws Exception {
                        // 每到一条数据, 用户的 pv 加一
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }).keyBy(r -> true)  // 为每一调数据分配同一个key,将聚合结果发送到一条流中
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1,
                                                       Tuple2<String, Long> value2) throws Exception {
                        // 将累加器更新为当前最大的pv统计值,然后向下游发送累加器的值
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                }).print();
        env.execute("run....");

    }

}

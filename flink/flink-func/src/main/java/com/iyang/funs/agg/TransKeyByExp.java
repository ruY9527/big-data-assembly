package com.iyang.funs.agg;

import com.iyang.funs.datas.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class TransKeyByExp {

    // keyBy 之后不再是 DataStream , 而是 KeyedStream , 分区流或者键控流
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );

        // 进行了分组后的操作
        stream.keyBy(r -> r.f0).sum(1).print();
        stream.keyBy(r -> r.f0).sum("f1").print();

        stream.keyBy(r -> r.f0).max(1).print();
        stream.keyBy(r -> r.f0).max("f1").print();

        stream.keyBy(r -> r.f0).min(1).print();
        stream.keyBy(r -> r.f0).min("f1").print();

        stream.keyBy(r -> r.f0).maxBy(1).print();
        stream.keyBy(r -> r.f0).maxBy("f1").print();

        stream.keyBy(r -> r.f0).minBy(1).print();
        stream.keyBy(r -> r.f0).min("f1").print();

        env.execute("run...");

    }


    public void commonExtraFun() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        // KeyedStream<Event, String> keyedStream = stream.keyBy(e -> e.user);
        KeyedStream<Event, Object> keyedStream = stream.keyBy(new KeySelector<Event, Object>() {

            @Override
            public Object getKey(Event value) throws Exception {
                return value.user;
            }

        });
        // min/max/sum/minBy/maxBy
        keyedStream.print();

        env.setParallelism(1).execute("run....");

    }

}

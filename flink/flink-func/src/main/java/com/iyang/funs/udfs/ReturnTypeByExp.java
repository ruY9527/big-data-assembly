package com.iyang.funs.udfs;

import com.iyang.funs.datas.Event;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class ReturnTypeByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        SingleOutputStreamOperator<Tuple2<String, Long>> operator =
                clicks.map(event -> Tuple2.of(event.user, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG));

        operator.print();
        env.execute("run...");
    }

}

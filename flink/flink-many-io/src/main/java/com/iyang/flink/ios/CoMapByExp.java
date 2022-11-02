package com.iyang.flink.ios;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/***
 * @author: baoyang
 * @data: 2022/11/2
 * @desc:
 ***/
public class CoMapByExp {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3);
        DataStreamSource<Long> longStream = env.fromElements(1L, 2L, 3L);

        ConnectedStreams<Integer, Long> connectedStreams = intStream.connect(longStream);

        SingleOutputStreamOperator<Object> resultStream = connectedStreams.map(new CoMapFunction<Integer, Long, Object>() {
            @Override
            public Object map1(Integer value) throws Exception {
                return "Integer: " + value;
            }

            @Override
            public Object map2(Long value) throws Exception {
                return "Long: " + value;
            }
        });

        resultStream.print();
        env.execute("runing...");

    }

}

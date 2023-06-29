package com.iyang.base.sources;


import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class ConnectStreams {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Integer> ds1 = env.socketTextStream("localhost", 8081)
                .map(i -> Integer.parseInt(i));
        DataStreamSource<String> ds2 = env.socketTextStream("localhost", 8082);

        ConnectedStreams<Integer, String> connect = ds1.connect(ds2);

        SingleOutputStreamOperator<String> result = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "来源于数字流:" + value;
            }

            @Override
            public String map2(String value) throws Exception {
                return "来源于字符流:" + value;
            }
        });

        result.print();

        env.execute();

    }


}

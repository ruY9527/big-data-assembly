package com.iyang.flink.ios;

import com.iyang.flink.ios.datas.ClickSource;
import com.iyang.flink.ios.datas.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: baoyang
 * @data: 2022/11/2
 * @desc:
 ***/
public class SplitStreamByFilter {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> maryStream = streamSource.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return "Mary".equalsIgnoreCase(value.user);
            }
        });


        SingleOutputStreamOperator<Event> bobStream = streamSource.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return "Bob".equalsIgnoreCase(value.user);
            }
        });

        maryStream.print();
        bobStream.print();

        env.execute("running....");

    }

}

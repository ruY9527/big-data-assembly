package com.iyang.flink.ios;

import com.iyang.flink.ios.datas.ClickSource;
import com.iyang.flink.ios.datas.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/***
 * @author: baoyang
 * @data: 2022/11/2
 * @desc: 使用侧边流来进行操作
 ***/
public class SplitStreamByOutputTag {

    private static OutputTag<Tuple3<String, String, Long>> MaryTag = new OutputTag<Tuple3<String,String,Long>>("Mary-pv"){};
    private static OutputTag<Tuple3<String, String, Long>>BobTag = new OutputTag<Tuple3<String,String,Long>>("Bob-pv"){};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Object> process = streamSource.process(new ProcessFunction<Event, Object>() {

            @Override
            public void processElement(Event value, Context ctx, Collector<Object> out) throws Exception {

                String user = value.user;
                if ("Mary".equalsIgnoreCase(user)) {
                    ctx.output(MaryTag, new Tuple3<>(value.user, value.url, value.timestamp));
                } else if ("Bob".equalsIgnoreCase(user)) {
                    ctx.output(BobTag, new Tuple3<>(value.user, value.url, value.timestamp));
                } else {
                    out.collect(value);
                }
            }

        });

        process.getSideOutput(MaryTag).print("Mary pv");
        process.getSideOutput(BobTag).print("Bob pv");
        process.print("else");

        env.execute("sideout running...");
    }

}

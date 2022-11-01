package com.iyang.funs.partions;

import com.iyang.funs.datas.Event;
import com.iyang.funs.sources.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class BroadcastPartitionByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());
        streamSource.broadcast().print("broadcast").setParallelism(4);
        env.execute("runing....");

    }

}

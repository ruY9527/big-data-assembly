package com.iyang.flinks;

import com.iyang.flinks.datas.MySource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: baoyang
 * @data: 2022/8/7
 * @desc:
 ***/
public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> sum =
                executionEnvironment.addSource(new MySource()).flatMap(new LineSplitter())
                        .keyBy(0)
                        // .timeWindow(Time.seconds(10))
                        .sum(1);

        sum.print();
        executionEnvironment.execute("StreamWorldCount");
    }

}

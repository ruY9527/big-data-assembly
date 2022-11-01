package com.iyang.funs;

import com.iyang.funs.common.UserExtractor;
import com.iyang.funs.datas.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class TransMapExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> elements = environment.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L));

        // 返回的是 SingleOutputStreamOperator
        // 表示map是一个用户可以自定义的转换(transformation)算子,它作用于一条数据流上,转换处理的结果是一个确定的输出类型
        elements.map(new UserExtractor()).print();
        environment.execute("run...");

    }

}

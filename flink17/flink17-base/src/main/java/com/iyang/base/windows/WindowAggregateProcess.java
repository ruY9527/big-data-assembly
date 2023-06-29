package com.iyang.base.windows;

import com.iyang.base.functions.WaterSensorMapFunction;
import com.iyang.base.pojos.WaterSensor;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class WindowAggregateProcess {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDs = env.socketTextStream("localhost", 8898).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKs = sensorDs.keyBy(s -> s.getId());

        WindowedStream<WaterSensor, String, TimeWindow> sensorWs = sensorKs.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<String> result = sensorWs.aggregate(
                new SelfAggFunction(),
                new SelfProcessWindowFunction()
        );

        result.print();
        env.execute();
    }

}

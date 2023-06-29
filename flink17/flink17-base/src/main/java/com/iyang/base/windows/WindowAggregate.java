package com.iyang.base.windows;

import com.iyang.base.functions.WaterSensorMapFunction;
import com.iyang.base.pojos.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
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
public class WindowAggregate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDs = env.socketTextStream("localhost", 8899).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKs = sensorDs.keyBy(s -> s.getId());


        WindowedStream<WaterSensor, String, TimeWindow> sensorWindow = sensorKs.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<String> aggregate = sensorWindow.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
            @Override
            public Integer createAccumulator() {
                System.out.println("累加器的创建");
                return 0;
            }

            @Override
            public Integer add(WaterSensor value, Integer accumulator) {
                System.out.println("调用add方法;value = " + value);
                return accumulator + value.getVc();
            }

            @Override
            public String getResult(Integer accumulator) {
                System.out.println("调用getResult方法");
                return accumulator.toString();
            }

            @Override
            public Integer merge(Integer a, Integer b) {
                System.out.println("调用merge方法");
                return null;
            }
        });

        aggregate.print();
        env.execute();

    }

}

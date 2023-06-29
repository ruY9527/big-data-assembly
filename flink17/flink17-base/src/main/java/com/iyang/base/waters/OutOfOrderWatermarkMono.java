package com.iyang.base.waters;

import com.iyang.base.functions.WaterSensorMapFunction;
import com.iyang.base.pojos.WaterSensor;
import com.iyang.base.utils.ProcessWindowFunctionUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class OutOfOrderWatermarkMono {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDs = env.socketTextStream("localhost", 8898).map(new WaterSensorMapFunction());

        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(((element, recordTimestamp) -> {
                    System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                    return element.getTs() * 1000L;
                }));

        SingleOutputStreamOperator<WaterSensor> sensorDsWater = sensorDs.assignTimestampsAndWatermarks(watermarkStrategy);
        sensorDsWater.keyBy(s -> s.getId()).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(ProcessWindowFunctionUtils.createWaterSensorFunction())
                .print();

        env.execute();

    }

}

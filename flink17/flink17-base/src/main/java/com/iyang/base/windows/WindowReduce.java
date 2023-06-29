package com.iyang.base.windows;

import com.iyang.base.functions.WaterSensorMapFunction;
import com.iyang.base.pojos.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class WindowReduce {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost",8899)
                .map(new WaterSensorMapFunction())
                .keyBy(r -> r.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("调用reduce方法，之前的结果:"+value1 + ",现在来的数据:"+value2);
                        return new WaterSensor(value1.getId(), System.currentTimeMillis(),value1.getVc() - value2.getVc());
                    }
                }).print();

        env.execute();

    }

}

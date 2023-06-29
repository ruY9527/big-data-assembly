package com.iyang.base.operators;

import com.iyang.base.pojos.WaterSensor;
import com.iyang.base.utils.WaterSensorUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class FlatMapOperators {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> source = env.fromCollection(WaterSensorUtils.generatorWater());

        source.flatMap(new UserFlatMap()).print();

        env.execute();


    }

}

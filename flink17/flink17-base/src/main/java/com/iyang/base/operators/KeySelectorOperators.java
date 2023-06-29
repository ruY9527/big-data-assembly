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
public class KeySelectorOperators {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> source = environment.fromCollection(WaterSensorUtils.generatorWater());

        source.keyBy(new UserKeySelector()).max("vc").print();

        environment.execute();



    }

}

package com.iyang.base.operators;

import com.iyang.base.pojos.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class UserFlatMap implements FlatMapFunction<WaterSensor,String> {


    @Override
    public void flatMap(WaterSensor waterSensor, Collector<String> collector) throws Exception {

        if ("sensor_1".equals(waterSensor.id)) {
            collector.collect(String.valueOf(waterSensor.vc * 98));
        } else if ("sensor_2".equals(waterSensor.id)) {
            collector.collect(waterSensor.ts+"");
            collector.collect(waterSensor.vc+"");
        }

    }

}

package com.iyang.base.operators;

import com.iyang.base.pojos.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class UserFilterMap implements FilterFunction<WaterSensor> {

    @Override
    public boolean filter(WaterSensor waterSensor) throws Exception {
        return "sensor_1".equals(waterSensor.id);
    }

}

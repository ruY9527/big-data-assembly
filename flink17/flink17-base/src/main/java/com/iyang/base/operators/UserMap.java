package com.iyang.base.operators;

import com.iyang.base.pojos.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class UserMap implements MapFunction<WaterSensor, String> {


    @Override
    public String map(WaterSensor waterSensor) throws Exception {
        return waterSensor.id;
    }


}

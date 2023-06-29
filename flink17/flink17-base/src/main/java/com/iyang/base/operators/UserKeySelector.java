package com.iyang.base.operators;

import com.iyang.base.pojos.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class UserKeySelector implements KeySelector<WaterSensor, String> {
    @Override
    public String getKey(WaterSensor waterSensor) throws Exception {
        return waterSensor.id;
    }
}

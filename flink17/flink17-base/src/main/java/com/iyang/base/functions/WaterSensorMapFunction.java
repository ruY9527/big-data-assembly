package com.iyang.base.functions;

import com.iyang.base.pojos.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class WaterSensorMapFunction implements MapFunction< String, WaterSensor> {


    @Override
    public WaterSensor map(String value) throws Exception {
        String[] datas = value.split(",");
        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
    }


}

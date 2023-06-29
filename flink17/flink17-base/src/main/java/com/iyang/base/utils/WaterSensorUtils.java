package com.iyang.base.utils;

import com.iyang.base.pojos.WaterSensor;

import java.util.Arrays;
import java.util.List;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class WaterSensorUtils {

    /**
     * 模拟集合数据
     * @return
     */
    public static List<WaterSensor> generatorWater(){

        return Arrays.asList(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

    }


}

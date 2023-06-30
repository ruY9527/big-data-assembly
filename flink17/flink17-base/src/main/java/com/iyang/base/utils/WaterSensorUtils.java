package com.iyang.base.utils;

import com.iyang.base.pojos.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple3;

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


    public static List<WaterSensor> generatorWater5(){

        return Arrays.asList(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 2L, 2),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3),
                new WaterSensor("s3", 4L, 4)
        );

    }


    public static List<Tuple3<String, Integer, Integer>> generatorWater3(){

        return Arrays.asList(
                Tuple3.of("zs", 80, 3),
                Tuple3.of("zs", 90, 4),
                Tuple3.of("zs", 95, 4),
                Tuple3.of("ls", 75, 4),
                Tuple3.of("ls", 65, 4),
                Tuple3.of("ls", 85, 4)
        );

    }

}

package com.iyang.base.windows;

import com.iyang.base.pojos.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class SelfAggFunction implements AggregateFunction<WaterSensor, Integer, String> {
    @Override
    public Integer createAccumulator() {
        System.out.println("累加器的创建");
        return 0;
    }

    @Override
    public Integer add(WaterSensor value, Integer accumulator) {
        System.out.println("调用add方法;value = " + value);
        return accumulator + value.getVc();
    }

    @Override
    public String getResult(Integer accumulator) {
        System.out.println("调用getResult方法");
        return accumulator.toString();
    }

    @Override
    public Integer merge(Integer a, Integer b) {
        System.out.println("调用merge方法");
        return null;
    }
}

package com.iyang.table.func;

import org.apache.flink.table.functions.AggregateFunction;

/***
 * @author: baoyang
 * @data: 2022/11/17
 * @desc:
 ***/
public class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccumulator> {

    @Override
    public Long getValue(WeightedAvgAccumulator accumulator) {

        if (accumulator.count == 0) {
            // 防止为0的情况
            return null;
        }
        // 计算平均值并返回
        return accumulator.sum / accumulator.count;
    }

    @Override
    public WeightedAvgAccumulator createAccumulator() {
        return new WeightedAvgAccumulator();
    }

    // 累加计算方法,每来一行数据都会调用
    public void accumulate(WeightedAvgAccumulator acc,Long iValue,Integer iWeight){

        acc.sum += iValue * iWeight;
        acc.count += iWeight;

    }

}

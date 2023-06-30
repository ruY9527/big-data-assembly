package com.iyang.base.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;

/****
 * author: BaoYang
 * date: 2023/6/30
 * desc:
 ***/
public class WeightedAvgTable extends AggregateFunction<Double, Tuple2<Integer, Integer>> {

    @Override
    public Double getValue(Tuple2<Integer, Integer> integerIntegerTuple2) {

        return integerIntegerTuple2.f0 * 1D / integerIntegerTuple2.f1;
    }

    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return Tuple2.of(0,0);
    }

    public void accumulate(Tuple2<Integer, Integer> acc, Integer score, Integer weight){

        acc.f0 += score * weight;
        acc.f1 += weight;
    }

}

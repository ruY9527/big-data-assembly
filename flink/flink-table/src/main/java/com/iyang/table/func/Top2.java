package com.iyang.table.func;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;


/***
 * @author: baoyang
 * @data: 2022/11/17
 * @desc:
 ***/
public class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accumulator> {

    @Override
    public Top2Accumulator createAccumulator() {
        Top2Accumulator acc = new Top2Accumulator();
        acc.first = Integer.MIN_VALUE;
        acc.second = Integer.MIN_VALUE;
        return acc;
    }

    public void accumulate(Top2Accumulator acc, Integer value){

        if (value > acc.first) {
            acc.second = acc.first;
            acc.first = value;
        } else if (value > acc.second) {
            acc.second = value;
        }

    }

    public void emitValue(Top2Accumulator acc, Collector<Tuple2<Integer, Integer>> out){

        if (acc.first != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.first, 1));
        }

        if (acc.second != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.second, 2));
        }

    }

}

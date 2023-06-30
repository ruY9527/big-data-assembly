package com.iyang.base.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/****
 * author: BaoYang
 * date: 2023/6/30
 * desc:
 ***/
public class Top2Function extends TableAggregateFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return Tuple2.of(0,0);
    }

    public void accumulate(Tuple2<Integer, Integer> acc , Integer num){

        if (num > acc.f0) {
            acc.f1 = acc.f0;
            acc.f0 = num;
        } else if (num > acc.f1) {
            acc.f1 = num;
        }

    }

    public void emitValue(Tuple2<Integer, Integer> acc , Collector<Tuple2<Integer, Integer>> out){

        if (acc.f0 != 0) {
            out.collect(Tuple2.of(acc.f0, 1));
        }
        if (acc.f1 != 0) {
            out.collect(Tuple2.of(acc.f1, 2));
        }

    }


}

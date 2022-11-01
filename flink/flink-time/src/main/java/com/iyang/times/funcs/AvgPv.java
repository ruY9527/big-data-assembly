package com.iyang.times.funcs;

import com.iyang.times.datas.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashSet;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class AvgPv implements AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double> {


    @Override
    public Tuple2<HashSet<String>, Long> createAccumulator() {
        // 创建累加器
        return Tuple2.of(new HashSet<String>(), 0L);
    }

    @Override
    public Tuple2<HashSet<String>, Long> add(Event value, Tuple2<HashSet<String>, Long> accumulator) {
        // 属于本窗口的数据来一条累加一次,并返回累加器

        accumulator.f0.add(value.user);
        return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
    }

    @Override
    public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
        // 闭合窗口的时候,增量聚合结果,将计算结果发送到下游
        // 计算且发送到下游的操作
        return (double) accumulator.f1 / accumulator.f0.size();
    }

    @Override
    public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
        return null;
    }
}

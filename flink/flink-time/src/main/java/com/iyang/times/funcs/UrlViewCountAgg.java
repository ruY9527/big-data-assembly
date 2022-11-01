package com.iyang.times.funcs;

import com.iyang.times.datas.Event;
import org.apache.flink.api.common.functions.AggregateFunction;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Event value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return null;
    }
}

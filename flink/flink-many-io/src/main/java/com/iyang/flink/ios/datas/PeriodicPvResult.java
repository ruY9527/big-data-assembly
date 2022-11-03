package com.iyang.flink.ios.datas;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/***
 * @author: baoyang
 * @data: 2022/11/3
 * @desc:
 ***/
public class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {
    ValueState<Long> countState;
    ValueState<Long> timerTsState;

    @Override
    public void open(Configuration parameters) throws Exception {

        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));

    }

    @Override
    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {

        Long count = countState.value();
        if (count == null) {
            countState.update(1L);
        }

    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
    }
}

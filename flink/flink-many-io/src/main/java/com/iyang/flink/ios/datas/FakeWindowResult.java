package com.iyang.flink.ios.datas;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/***
 * @author: baoyang
 * @data: 2022/11/3
 * @desc:
 ***/
public class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {

    private Long windowSize;
    MapState<Long, Long> windowPvMapState;

    public FakeWindowResult() {
    }

    public FakeWindowResult(Long windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        windowPvMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-pv", Long.class, Long.class));
    }

    @Override
    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {

        Long windowStart = value.timestamp / windowSize * windowSize;
        Long windowEnd = windowStart + windowStart;

        ctx.timerService().registerEventTimeTimer(windowEnd -1);
        if (windowPvMapState.contains(windowStart)) {
            Long pv = windowPvMapState.get(windowStart);
            windowPvMapState.put(windowStart, pv + 1);
        } else {
            windowPvMapState.put(windowStart, 1L);
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        Long windowEnd = timestamp + 1;
        Long windowStart = windowEnd - windowSize;
        Long pv = windowPvMapState.get(windowStart);
        out.collect("url: " + ctx.getCurrentKey() + " 访问量: " + pv + " 窗口:" + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd));
        windowPvMapState.remove(windowStart);

    }
}

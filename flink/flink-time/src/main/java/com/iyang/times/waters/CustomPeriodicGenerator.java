package com.iyang.times.waters;

import com.iyang.times.datas.Event;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class CustomPeriodicGenerator implements WatermarkGenerator<Event> {

    // 延迟时间
    private Long delayTime = 5000L;

    // 观察到的最大时间戳
    private Long maxTs = Long.MIN_VALUE + delayTime + 1L;

    @Override
    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {

        // 每来一条数据就调用一次
        // 最大时间戳
        maxTs = Math.max(event.timestamp, maxTs);

    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

        // 发射水位线, 默认 200 ms 调用一次
        // 由系统统一调用,默认200ms,发出水位线
        output.emitWatermark(new Watermark(maxTs - delayTime - 1L));

    }


}

package com.iyang.base.waters;

import com.iyang.base.pojos.Event;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class SelfBoundedOutOfOrdernessGenerator implements WatermarkGenerator<Event> {

    // 延迟时间
    private Long delayTime = 5000L;

    private Long maxTs = Long.MAX_VALUE + delayTime + 1L;

    @Override
    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {

        maxTs = Math.max(event.timestamp, maxTs);

    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

        // 发射水位线,默认 200 ms
        output.emitWatermark(new Watermark(maxTs - delayTime -1L));

    }
}

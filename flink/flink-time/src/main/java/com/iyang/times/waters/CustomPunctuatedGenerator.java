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
public class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {
    @Override
    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {

        // 根据判断条件,发出水位线
        if ("Mary".equalsIgnoreCase(event.user)) {
            output.emitWatermark(new Watermark(event.timestamp -1));
        }

    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

    }
}

package com.iyang.base.waters;

import com.iyang.base.pojos.Event;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class SelfWatermarkStrategy implements WatermarkStrategy<Event> {
    @Override
    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return null;
    }
}

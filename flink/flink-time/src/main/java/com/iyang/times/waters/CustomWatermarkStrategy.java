package com.iyang.times.waters;

import com.iyang.times.datas.Event;
import org.apache.flink.api.common.eventtime.*;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class CustomWatermarkStrategy implements WatermarkStrategy<Event> {

    @Override
    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        new SerializableTimestampAssigner<Event>(){
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        };

        return null;
    }

    @Override
    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new CustomPeriodicGenerator();
    }


}

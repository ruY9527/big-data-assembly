package com.iyang.times.triggers;

import com.iyang.times.datas.Event;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class MyTrigger extends Trigger<Event, TimeWindow> {
    @Override
    public TriggerResult onElement(Event element, long timestamp,
                                   TimeWindow window, TriggerContext ctx) throws Exception {
        ValueState<Boolean> valueState = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN()));
        if (valueState.value() == null) {

            for(long i = window.getStart(); i < window.getEnd(); i = i + 1000L){
                ctx.registerEventTimeTimer(i);
            }
            valueState.update(true);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        ValueState<Boolean> state = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN()));
        state.clear();

    }
}

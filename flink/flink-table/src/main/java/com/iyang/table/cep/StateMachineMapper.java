package com.iyang.table.cep;

import com.iyang.table.objs.LoginEvent;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.network.ChannelState;

/***
 * @author: baoyang
 * @data: 2022/11/17
 * @desc:
 ***/
public class StateMachineMapper extends RichFlatMapFunction<LoginEvent, String> {

    private ValueState<State> currentState;

    @Override
    public void open(Configuration parameters) throws Exception {
        currentState = getRuntimeContext().getState(new ValueStateDescriptor<>("state", State.class));
    }

    @Override
    public void flatMap(LoginEvent value, Collector<String> out) throws Exception {
        State state = currentState.value();

        if (state == null) {
            // ChannelState.State.
        }

    }
}

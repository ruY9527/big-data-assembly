package com.iyang.flink.ios.func;

import jdk.nashorn.internal.codegen.types.Type;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/***
 * @author: baoyang
 * @data: 2022/11/2
 * @desc:
 ***/
public class OrderMatchResult extends CoProcessFunction<Tuple3<String,String,Long>, Tuple4<String,String,String,Long>, String> {

    private ValueState<Tuple3<String, String, Long>> appEventState;
    private ValueState<Tuple4<String,String ,String , Long>> thirdPartyEventState;

    @Override
    public void open(Configuration parameters) throws Exception {

        appEventState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event",
                Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));

        thirdPartyEventState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple4<String, String, String, Long>>("thirdparty-event",
                Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)
        ));

    }

    @Override
    public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {


        if(thirdPartyEventState.value() != null){

            out.collect("对账成功: " + value + " " + thirdPartyEventState.value());
            thirdPartyEventState.clear();

        } else  {
            appEventState.update(value);
            ctx.timerService().registerEventTimeTimer(value.f2 + 5000L );
        }

    }

    @Override
    public void processElement2(Tuple4<String, String, String, Long> value,
                                Context ctx, Collector<String> out) throws Exception {

        if (appEventState.value() != null) {
            out.collect("对账成功: " + appEventState.value() + " " + value);
            appEventState.clear();
        } else {
            // 更新状态
            thirdPartyEventState.update(value);
            ctx.timerService().registerEventTimeTimer(value.f3 + 5000L);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        if (appEventState.value() != null) {
            out.collect("对账失败: " + appEventState.value() + " " + "第三方支付平台信息未到");
        }

        if (thirdPartyEventState.value() != null) {
            out.collect("对账失败: " + thirdPartyEventState.value() + " " + "app信息未到");
        }

        appEventState.clear();
        thirdPartyEventState.clear();

    }
}

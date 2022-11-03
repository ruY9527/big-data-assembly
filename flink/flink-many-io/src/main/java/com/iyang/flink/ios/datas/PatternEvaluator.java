package com.iyang.flink.ios.datas;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/***
 * @author: baoyang
 * @data: 2022/11/3
 * @desc:
 ***/
public class PatternEvaluator extends KeyedBroadcastProcessFunction<String, Action,Pattern, Tuple2<String, Pattern>> {

    // 定义一个值的状态,保存上一次用户行为
    ValueState<String> prevActionState;

    @Override
    public void processElement(Action value, ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {

        prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastAction", Types.STRING()));

    }

    @Override
    public void processBroadcastElement(Pattern value, Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {

        // ctx.getBroadcastState(new MapStateDescriptor<>("patterns", Types.VOID , Types.POJO(Pattern.class)));

    }
}

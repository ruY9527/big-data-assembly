package com.iyang.table.cep;

import com.iyang.table.objs.OrderEvent;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/***
 * @author: baoyang
 * @data: 2022/11/17
 * @desc:
 ***/
public class OrderPayPatternProcessFunction extends PatternProcessFunction<OrderEvent, String>
        implements TimedOutPartialMatchHandler<OrderEvent> {
    @Override
    public void processMatch(Map<String, List<OrderEvent>> match, Context ctx, Collector<String> out) throws Exception {

        OrderEvent orderEvent = match.get("pay").get(0);
        out.collect("订单 " + orderEvent.orderId + "已支付!");

    }

    @Override
    public void processTimedOutMatch(Map<String, List<OrderEvent>> match, Context ctx) throws Exception {

        OrderEvent orderEvent = match.get("create").get(0);
        ctx.output(new OutputTag<String>("timeout"){},
                "订单" + orderEvent.orderId + " 超时未支付!用户为: " + orderEvent.userId);

    }
}

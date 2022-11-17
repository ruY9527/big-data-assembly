package com.iyang.table.cep;

import com.iyang.table.objs.Event;
import com.iyang.table.objs.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/***
 * @author: baoyang
 * @data: 2022/11/17
 * @desc:
 ***/
public class LoginFailDetect {

    /**
     * oneAndMore()  匹配事件出现一次或者多次
     * times(times)  匹配事件发生特定次数,  a.times(3) 表示 aaa
     * times(fromTimes, toTimes) 指定匹配事件出现的次数范围,最小次数为 fromTimes,最大次数为toTimes. a.times(2,4)可以匹配aa,aaa和aaaa
     * greedy()    只能使用在贪心模式后,使当前循环模式变得"贪心"(greedy),也就是总可能多的去匹配。 a.times(2,4),如果出现连续4个a,那么会直接把aaaa检测出来进行处理,
     * optionas()  使用当前模式成为可选的,也就说可以满足这个匹配条件,也可以不满足
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<LoginEvent, String> keyedStream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })).keyBy(r -> r.userId);

        // 连续三次登录失败事件检测
        // next 紧接着的事件
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first").where(new SimpleCondition<LoginEvent>() {
            // 个体模式,基本形式
            // 简单条件的匹配
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.eventType);
            }
        }).next("second").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.eventType);
            }
        }).next("third").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.eventType);
            }
        });

        // 使用 times 连续三次来 进行替换
        Pattern<LoginEvent, LoginEvent> fails = Pattern.<LoginEvent>begin("fails").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.eventType);
            }
        }).times(3).consecutive();

        //
        Pattern.<Event>begin("a").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return "a".equals(value.user);
            }
        }).oneOrMore().followedBy("b").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return "b".equals(value.user);
            }
        });

        // 将 Pattern 应用到流上, 检测匹配的负责事件上,得到一个 PatternStream
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);

        patternStream.select(
                new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> map) throws Exception {
                        LoginEvent first = map.get("first").get(0);
                        LoginEvent second = map.get("second").get(0);
                        LoginEvent third = map.get("third").get(0);

                        return first.userId + "连续三次登录失败: " + first.timestamp + " ; " + second.timestamp + " ; "
                                + third.timestamp;
                    }
                }
        ).print("warning");

        env.execute("runing");

    }

}

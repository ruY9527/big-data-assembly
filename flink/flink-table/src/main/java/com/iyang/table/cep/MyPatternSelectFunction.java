package com.iyang.table.cep;

import com.iyang.table.objs.Event;
import org.apache.flink.cep.PatternSelectFunction;

import java.util.List;
import java.util.Map;

/***
 * @author: baoyang
 * @data: 2022/11/17
 * @desc:
 ***/

public class MyPatternSelectFunction implements PatternSelectFunction<Event, String> {


    @Override
    public String select(Map<String, List<Event>> pattern) throws Exception {
        Event start = pattern.get("start").get(0);
        Event middle = pattern.get("middle").get(0);
        return start.toString() + " " + middle.toString();
    }

}

package com.iyang.funs.common;

import com.iyang.funs.datas.Event;
import org.apache.flink.api.common.functions.MapFunction;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class UserExtractor implements MapFunction<Event, String> {

    @Override
    public String map(Event event) throws Exception {
        return event.user;
    }

}

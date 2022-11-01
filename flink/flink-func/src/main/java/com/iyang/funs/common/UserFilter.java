package com.iyang.funs.common;

import com.iyang.funs.datas.Event;
import org.apache.flink.api.common.functions.FilterFunction;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class UserFilter implements FilterFunction<Event> {

    // 对条件进行过滤
    @Override
    public boolean filter(Event value) throws Exception {
        return value.user.length() > 3;
    }

}

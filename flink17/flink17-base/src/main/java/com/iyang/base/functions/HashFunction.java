package com.iyang.base.functions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

/****
 * author: BaoYang
 * date: 2023/6/30
 * desc:
 ***/
public class HashFunction extends ScalarFunction {

    public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o){

        return o.hashCode();
    }

}

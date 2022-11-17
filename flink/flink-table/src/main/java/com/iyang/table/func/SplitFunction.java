package com.iyang.table.func;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/***
 * @author: baoyang
 * @data: 2022/11/17
 * @desc:
 ***/

// @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str){

        for (String s : str.split(" ")) {

        }

    }

}

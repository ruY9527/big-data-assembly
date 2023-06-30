package com.iyang.base.functions;

import com.iyang.base.tables.MyTableFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;

/****
 * author: BaoYang
 * date: 2023/6/30
 * desc:
 ***/

@FunctionHint(output = @DataTypeHint("ROW<word STRING,length INT>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str){

        Arrays.stream(str.split(" ")).forEach(word -> collect(Row.of(word, word.length())));

    }

}

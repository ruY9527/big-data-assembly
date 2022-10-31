package com.iyang.flinks.sockets.func;

import com.iyang.flinks.datas.WordCountObj;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/***
 * @author: baoyang
 * @data: 2022/10/31
 * @desc: 抽取出业务相关的数据来
 ***/

public class SpiltWordFuns implements FlatMapFunction<String, WordCountObj> {

    @Override
    public void flatMap(String s, Collector<WordCountObj> collector) throws Exception {

        String[] fields = s.split(",");
        for (String field : fields) {
            collector.collect(new WordCountObj(field, 1));
        }    
        
    }

}

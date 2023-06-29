package com.iyang.base.functions;

import org.apache.flink.connector.datagen.source.GeneratorFunction;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class FileGeneratorFunction implements GeneratorFunction<Long, String> {


    @Override
    public String map(Long value) throws Exception {
        return "Number:" + value;
    }

}

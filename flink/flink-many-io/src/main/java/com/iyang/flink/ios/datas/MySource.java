package com.iyang.flink.ios.datas;


import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/***
 * @author: baoyang
 * @data: 2022/8/7
 * @desc:
 ***/
public class MySource implements SourceFunction<String> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        while (isRunning) {
            List<String> stringList = new ArrayList<>();
            stringList.add("world");
            stringList.add("Flink");
            stringList.add("Steam");
            stringList.add("Batch");
            stringList.add("Table");
            stringList.add("SQL");
            stringList.add("hello");
            int size = stringList.size();
            int nextInt = new Random().nextInt(size);
            sourceContext.collect(stringList.get(nextInt));
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}

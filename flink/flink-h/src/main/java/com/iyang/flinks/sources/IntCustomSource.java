package com.iyang.flinks.sources;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/***
 * @author: baoyang
 * @data: 2022/10/31
 * @desc:
 ***/
public class IntCustomSource implements ParallelSourceFunction<Integer> {

    public volatile boolean running = true;
    Random random = new Random();

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {

        while (running) {

            ctx.collect(random.nextInt());

        }

    }

    @Override
    public void cancel() {

        running = false;

    }


}

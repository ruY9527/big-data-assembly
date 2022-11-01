package com.iyang.times.datas;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/***
 * @author: baoyang
 * @data: 2022/10/31
 * @desc:  自定义数据源场景
 ***/
public class ClickSource implements SourceFunction<Event> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {

        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (running) {

            ctx.collect(new Event(users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()));
            Thread.sleep(1000);

        }
    }

    @Override
    public void cancel() {
        running = false;
    }


}

package com.iyang.times.waters;

import com.iyang.times.datas.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class ClickSourceWithWatermark implements SourceFunction<Event> {

    private boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {

        Random random = new Random();
        String[] userArr = {"Mary", "Bob", "Alice"};
        String[] urlArr = {"./home", "./cart", "./prod?id=1"};

        while (running) {

            // 毫秒时间戳
            long millis = Calendar.getInstance().getTimeInMillis();
            String userName = userArr[random.nextInt(userArr.length)];
            String url = urlArr[random.nextInt(urlArr.length)];
            Event event = new Event(userName, url, millis);

            // 使用 collectWithTimestamp 方法将数据,并指明数据中的时间戳字段
            ctx.collectWithTimestamp(event, event.timestamp);
            // 发送水位线
            ctx.emitWatermark(new Watermark(event.timestamp - 1));
            Thread.sleep(1000L);
        }

    }

    @Override
    public void cancel() {
        running = false;
    }
}

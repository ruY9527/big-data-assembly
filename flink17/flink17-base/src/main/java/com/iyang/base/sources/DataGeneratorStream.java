package com.iyang.base.sources;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class DataGeneratorStream {

    public static void main(String[] args) throws Exception  {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataGeneratorSource<String> source = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number:" + value;
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                Types.STRING
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "datagenerator").print();

        env.execute();


    }

}

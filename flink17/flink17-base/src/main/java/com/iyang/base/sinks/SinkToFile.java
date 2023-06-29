package com.iyang.base.sinks;

import com.iyang.base.functions.FileGeneratorFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class SinkToFile {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(new FileGeneratorFunction(), Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1000), Types.STRING);

        DataStreamSource<String> dataGen = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");

        FileSink<String> fileSink = FileSink.<String>forRowFormat(new Path("/home/luohong/coding/java/github_self/big-data-assembly/flink17"),
                        new SimpleStringEncoder<>("UTF-8")).withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("baoyang-")
                                .withPartSuffix(".log").build()
                ).withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMillis(1))
                                .withMaxPartSize(new MemorySize(1024 * 1024))
                                .build()
                ).build();

        dataGen.sinkTo(fileSink);

        env.execute();

    }

}

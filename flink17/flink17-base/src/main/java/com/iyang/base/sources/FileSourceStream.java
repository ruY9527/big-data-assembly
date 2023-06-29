package com.iyang.base.sources;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class FileSourceStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> build = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("/home/luohong/coding/java/github_self/big-data-assembly/flink17/flink17-base/files/words.txt")).build();
        env.fromSource(build, WatermarkStrategy.noWatermarks(), "file").print();

        env.execute();

    }


}

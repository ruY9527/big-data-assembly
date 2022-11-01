package com.iyang.funs.partions;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/***
 * @author: baoyang
 * @data: 2022/11/1
 * @desc:
 ***/
public class RescalePartitionByExp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new RichParallelSourceFunction<Integer>() {

            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {

                for (int i = 0; i < 8; i++) {
                    if ((i + 1) % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(i + 1);
                    }
                }

            }

            @Override
            public void cancel() {

            }

        }).setParallelism(2).rescale().print().setParallelism(4);
        env.execute("runing....");

    }

}

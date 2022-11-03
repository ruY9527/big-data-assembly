package com.iyang.flink.ios.datas;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/***
 * @author: baoyang
 * @data: 2022/11/3
 * @desc:
 ***/
public class BufferingSink implements SinkFunction<Event> , CheckpointedFunction {

    private  int threshold;
    private transient ListState<Event> checkpointedState;
    private List<Event> bufferedElements;

    public BufferingSink() {
    }

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void invoke(Event value, Context context) throws Exception {

        bufferedElements.add(value);
        if (bufferedElements.size() == threshold) {
            for (Event bufferedElement : bufferedElements) {
                System.out.println(bufferedElement);
            }
            System.out.println("=====输出完毕====");
            bufferedElements.clear();
        }

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        checkpointedState.clear();
        for (Event bufferedElement : bufferedElements) {
            checkpointedState.add(bufferedElement);
        }

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        ListStateDescriptor descriptor = new ListStateDescriptor<>("buffered-elements", Types.POJO(Event.class));
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (Event event : checkpointedState.get()) {
                bufferedElements.add(event);
            }
        }

    }
}

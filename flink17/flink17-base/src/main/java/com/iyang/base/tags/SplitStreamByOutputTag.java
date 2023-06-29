package com.iyang.base.tags;

import com.iyang.base.functions.WaterSensorMapFunction;
import com.iyang.base.pojos.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc: 分流
 ***/
public class SplitStreamByOutputTag {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("localhost", 7788)
                .map(new WaterSensorMapFunction());

        OutputTag<WaterSensor> s1 = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2 = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        // 返回主流
        SingleOutputStreamOperator<Object> ds1 = ds.process(new ProcessFunction<WaterSensor, Object>() {

            @Override
            public void processElement(WaterSensor value,
                                       ProcessFunction<WaterSensor, Object>.Context ctx,
                                       Collector<Object> out) throws Exception {
                if ("s1".equalsIgnoreCase(value.getId())) {
                    ctx.output(s1, value);
                } else if ("s2".equalsIgnoreCase(value.getId())) {
                    ctx.output(s2, value);
                } else {
                    out.collect(value);
                }
            }
        });

        ds1.print("主流 , 非s1,s2传感器");
        SideOutputDataStream<WaterSensor> s1DS = ds1.getSideOutput(s1);
        SideOutputDataStream<WaterSensor> s2DS = ds1.getSideOutput(s2);

        s1DS.printToErr("s1");
        s2DS.printToErr("s2");

        env.execute();

    }

}

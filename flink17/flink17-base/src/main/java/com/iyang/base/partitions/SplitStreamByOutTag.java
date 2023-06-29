package com.iyang.base.partitions;

import com.iyang.base.pojos.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class SplitStreamByOutTag {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        return new WaterSensor(value, 1L, 1);
                    }
                });

        OutputTag<WaterSensor> s1 = new OutputTag<WaterSensor>("s1", Types.POJO(WaterSensor.class)) {
        };

        OutputTag<WaterSensor> s2 = new OutputTag<WaterSensor>("s2", Types.POJO(WaterSensor.class)) {
        };


        SingleOutputStreamOperator<WaterSensor> d1 = source.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor waterSensor,
                                       ProcessFunction<WaterSensor, WaterSensor>.Context context,
                                       Collector<WaterSensor> collector) throws Exception {

                if ("s1".equals(waterSensor.id)) {
                    context.output(s1, waterSensor);
                } else if ("s2".equals(waterSensor.id)) {
                    context.output(s2, waterSensor);
                } else {
                    collector.collect(waterSensor);
                }

            }
        });

        d1.print("主流; 非s1,s2的传感器");
        SideOutputDataStream<WaterSensor> s1Ds = d1.getSideOutput(s1);
        SideOutputDataStream<WaterSensor> s2Ds = d1.getSideOutput(s2);

        s1Ds.printToErr("s1");
        s1Ds.printToErr("s2");

        env.execute();

    }

}

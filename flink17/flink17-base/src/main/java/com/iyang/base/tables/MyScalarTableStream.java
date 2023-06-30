package com.iyang.base.tables;

import com.iyang.base.functions.HashFunction;
import com.iyang.base.pojos.WaterSensor;
import com.iyang.base.utils.WaterSensorUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/****
 * author: BaoYang
 * date: 2023/6/30
 * desc:
 ***/
public class MyScalarTableStream {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> sensorDs = env.fromCollection(WaterSensorUtils.generatorWater5());

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table sensorTable = tableEnv.fromDataStream(sensorDs);

        tableEnv.createTemporaryView("sensor", sensorTable);
        tableEnv.createFunction("HashFunction", HashFunction.class);

        sensorTable.select(call("HashFunction", $("id"))).execute().print();

    }

}

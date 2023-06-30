package com.iyang.base.tables;

import com.iyang.base.pojos.WaterSensor;
import com.iyang.base.utils.WaterSensorUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/****
 * author: BaoYang
 * date: 2023/6/30
 * desc:
 ***/
public class TableStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> sourceDs = env.fromCollection(WaterSensorUtils.generatorWater5());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table sensorTable = tableEnv.fromDataStream(sourceDs);
        tableEnv.createTemporaryView("sensor", sensorTable);

        Table filterTable = tableEnv.sqlQuery("select id,ts,vc from sensor where ts > 2");
        Table sumTable = tableEnv.sqlQuery("select id,sum(vc) from sensor group by id");

        tableEnv.toDataStream(filterTable, WaterSensor.class).print("filter");
        tableEnv.toChangelogStream(sumTable).print("sum");

        env.execute();
    }

}

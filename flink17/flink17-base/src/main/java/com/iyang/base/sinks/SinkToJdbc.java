package com.iyang.base.sinks;

import com.iyang.base.functions.WaterSensorMapFunction;
import com.iyang.base.pojos.WaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/****
 * author: BaoYang
 * date: 2023/6/29
 * desc:
 ***/
public class SinkToJdbc {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDs =
                environment.socketTextStream("localhost", 8899).map(new WaterSensorMapFunction());

        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink(
                "insert into test01 values(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());
                    }
                },
                JdbcExecutionOptions.builder().withMaxRetries(3).withBatchSize(100)
                        .withBatchIntervalMs(3000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://127.0.0.1:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("YdMysqlxx9")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        );

        sensorDs.addSink(jdbcSink);
        environment.execute();

    }

}

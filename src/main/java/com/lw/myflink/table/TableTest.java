package com.lw.myflink.table;

import com.lw.myflink.Sensor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;

public class TableTest {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("D:\\study\\MyFlink\\src\\main\\resources\\sensor");
        SingleOutputStreamOperator<Sensor> map = stringDataStreamSource.map(
                e -> {
                    String[] split = e.split(" ");
                    return new Sensor(split[0], Double.valueOf(split[1]), Long.valueOf(split[2]));
                }
        );
        //创建Table执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironmentImpl.
                create(executionEnvironment, EnvironmentSettings.newInstance().build(),new TableConfig());
        Table table = streamTableEnvironment.fromDataStream(map);
        //调用Table进行转换
        Table select = table.select("id, temperature")
                .filter("id == 'sensor_1'");
        DataStream<Sensor> sensorDataStream = streamTableEnvironment.toAppendStream(select, Sensor.class);
        sensorDataStream.print();


    }
}

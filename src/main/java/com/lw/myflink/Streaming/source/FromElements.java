package com.lw.myflink.Streaming.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从给定的对象序列中创建DataStream,formElements 中的元素类型要一致
 */
public class FromElements {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.fromElements("a", "b", "c", "d", "a", "b");
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = dataSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.keyBy(0).sum(1);
        sum.print();
        env.execute("DataStreamFromElements");
    }
}

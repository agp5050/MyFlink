package com.lw.myflink.Streaming.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 从集合中创建DataStream ,一行行处理，每行读取都会输出结果
 */
public class FromCollectionSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.fromCollection(Arrays.asList("a", "a", "b", "b", "c", "c", "c", "c", "c"));
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = dataSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.keyBy(0).sum(1);
//        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = map.keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
//                return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
//            }
//        });
        sum.print();
        env.execute("DataStreamFromCollection");
    }
}


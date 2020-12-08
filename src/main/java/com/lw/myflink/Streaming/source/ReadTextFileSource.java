package com.lw.myflink.Streaming.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  从文件中读取数据得到 DataStream
 */
public class ReadTextFileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.readTextFile("./data/streamTextFile");
        SingleOutputStreamOperator<Tuple2<String, Integer>> faltMap = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = faltMap.keyBy(0).sum(1);
        sum.print();
        env.execute("Streaming WordCount");

    }
}

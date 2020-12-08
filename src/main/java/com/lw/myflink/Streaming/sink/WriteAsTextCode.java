package com.lw.myflink.Streaming.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 将DataStream结果保存到文件中，以读取socket为例
 * 代码中没有使用window，统计的就是自从程序启动以来通过socket传输的数据对象总数
 */
public class WriteAsTextCode {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("node5", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = socketStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = flatMap.keyBy(0).sum(1);
        DataStreamSink<Tuple2<String, Integer>> sink = result.writeAsText("./TempResult/result", FileSystem.WriteMode.OVERWRITE);
        env.execute("writeAsTextFile");
    }
}

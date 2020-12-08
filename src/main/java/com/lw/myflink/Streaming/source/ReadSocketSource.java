package com.lw.myflink.Streaming.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 读取socket数据源实现WordCount 得到DataStream
 */
public class ReadSocketSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("node5", 9999).setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> pairWords = socketStream.flatMap(new Splitter()).setParallelism(1);
        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = pairWords.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowStream = keyBy.timeWindow(Time.seconds(5));
        DataStream<Tuple2<String, Integer>> dataStream = windowStream.sum(1).setParallelism(1);
        dataStream.print().setParallelism(1);
        env.execute("socket wordcount");
    }

    //Splitter 实现了 FlatMapFunction ，将输入的一行数据按照空格进行切分，返回tuple<word,1>
    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}

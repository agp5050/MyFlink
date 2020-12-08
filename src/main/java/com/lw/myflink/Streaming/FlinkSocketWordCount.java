package com.lw.myflink.Streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 当前读取Socket数据，指定参数 --port 统计wordCount
 *
 */
public class FlinkSocketWordCount {

    public static void main(String[] args) throws Exception {
        final int port ;
        try{
            final ParameterTool params = ParameterTool.fromArgs(args); // --port 9999
            port = params.getInt("port");
        }catch (Exception e){
            System.err.println("No port specified. Please run 'FlinkSocketWordCount --port <port>'");
            return;
        }
        //获取执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从socket中获取数据。
        DataStreamSource<String> text  = env.socketTextStream("mynode5", port);

        SingleOutputStreamOperator<WordWithCount> wordWithCountInfos = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String line, Collector<WordWithCount> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(new WordWithCount(word, 1L));
                }
            }
        });
        //keyBy中所写的字段必须是类WordWithCount中的字段,WordWithCount中如果重写构造必须写上无参构造
        KeyedStream<WordWithCount, Tuple> keyedInfos = wordWithCountInfos.keyBy("word");
        WindowedStream<WordWithCount, Tuple, TimeWindow> windowedInfo = keyedInfos.timeWindow(Time.seconds(5),Time.seconds(1));

        SingleOutputStreamOperator<WordWithCount> windowCounts = windowedInfo.reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount w1, WordWithCount w2) throws Exception {
                return new WordWithCount(w1.getWord(), w1.getCount() + w2.getCount());
            }
        });

        windowCounts.print();

        env.execute("Socket Window WordCount");

    }

    public static class WordWithCount {
        public String word;
        public Long count;

        public WordWithCount() { }

        public WordWithCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }


        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}

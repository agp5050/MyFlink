package com.lw.myflink.SpecifyingKeys;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;

/**
 * 流式处理中定义tuple来指定key，使用keyBy:
 *  1.可以指定tuple中的哪个元素当做key
 *  2.可以指定tuple中联合的元素当做key
 *
 *  socket 输入数据举例：
 *      hello flink 0
 *      hello flink 1
 *      hello java 0
 */
public class KeyByUseTuple {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketText = env.socketTextStream("mynode5", 9999);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> map =
                socketText.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String line) throws Exception {
                return new Tuple3<>(line.split(" ")[0], line.split(" ")[1], Integer.valueOf(line.split(" ")[2]));
            }
        });
        //按照tuple的第1个元素当做key
//        KeyedStream<Tuple3<String, String, Integer>, Tuple> keyedStream = map.keyBy(0);
        //按照tuple的第2个元素当做key
//        KeyedStream<Tuple3<String, String, Integer>, Tuple> keyedStream = map.keyBy(1);
        //按照tuple的0,2元素当做key
        KeyedStream<Tuple3<String, String, Integer>, Tuple> keyedStream = map.keyBy(0,2);

        WindowedStream<Tuple3<String, String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(5));
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> outputStream =
                windowedStream.reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> tp1, Tuple3<String, String, Integer> tp2) throws Exception {
                return new Tuple3<>(tp1.f0 + tp2.f0, tp1.f1 + tp2.f1, tp1.f2 + tp2.f2);
            }
        });
        outputStream.print();
        env.execute("stream-tupleTest");

    }
}


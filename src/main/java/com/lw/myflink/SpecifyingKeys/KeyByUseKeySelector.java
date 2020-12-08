package com.lw.myflink.SpecifyingKeys;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 批处理中使用key selector来指定key，使用groupBy:
 *   使用key Selector这种方式选择key，非常方便，可以从数据类型中指定想要的key.
 *
 * socket测试数据：
 *  xiaoming	18	m	2	1	2	3
 *  xiaohong	19	f	1	4	5	3
 *  xiaoli	20	m	1	7	8	9
 */
public class KeyByUseKeySelector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketText = env.socketTextStream("mynode5", 9999);
        KeyedStream<String, String> keyBy = socketText.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String line) throws Exception {
                return line.split("\t")[2];
            }
        });
        WindowedStream<String, String, TimeWindow> window = keyBy.timeWindow(Time.seconds(5));
        SingleOutputStreamOperator<String> reduce = window.reduce(new ReduceFunction<String>() {
            @Override
            public String reduce(String s1, String s2) throws Exception {
                return s1 + "#" + s2;
            }
        });
        reduce.print();
        env.execute("KeySelectorTest");
    }
}

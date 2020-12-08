package com.lw.myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class ReadMessageFromKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);//设置并行度
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "mynode1:9092,mynode2:9092,mynode3:9092");
        props.setProperty("group.id", "flink-group");
        /**
         * 第一个参数是topic
         * 第二个参数是value的反序列化格式
         * 第三个参数是kafka配置
         */
        FlinkKafkaConsumer011<String> consumer011 =  new FlinkKafkaConsumer011<>("FlinkTopic", new SimpleStringSchema(), props);
        DataStreamSource<String> stringDataStreamSource = env.addSource(consumer011);
        SingleOutputStreamOperator<String> flatMap = stringDataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> outCollector) throws Exception {
                String[] split = s.split(" ");
                for (String currentOne : split) {
                    outCollector.collect(currentOne);
                }
            }
        });
        //注意这里的tuple2需要使用org.apache.flink.api.java.tuple.Tuple2 这个包下的tuple2
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });
        //keyby 将数据根据key 进行分区，保证相同的key分到一起，默认是按照hash 分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByResult = map.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowResult = keyByResult.timeWindow(Time.seconds(5));
        SingleOutputStreamOperator<Tuple2<String, Integer>> endResult = windowResult.sum(1);

        //sink 直接控制台打印
        //执行flink程序，设置任务名称。console 控制台每行前面的数字代表当前数据是哪个并行线程计算得到的结果
//        endResult.print();

        //sink 将结果存入文件,FileSystem.WriteMode.OVERWRITE 文件目录存在就覆盖
//        endResult.writeAsText("./result/kafkaresult",FileSystem.WriteMode.OVERWRITE);
//        endResult.writeAsText("./result/kafkaresult",FileSystem.WriteMode.NO_OVERWRITE);

        //sink 将结果存入kafka topic中,存入kafka中的是String类型，所有endResult需要做进一步的转换
        FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011<>("mynode1:9092,mynode2:9092,mynode3:9092","FlinkResult",new SimpleStringSchema());
        //将tuple2格式数据转换成String格式
        endResult.map(new MapFunction<Tuple2<String,Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> tp2) throws Exception {
                return tp2.f0+"-"+tp2.f1;
            }
        }).addSink(producer);

        //最后要调用execute方法启动flink程序
        env.execute("kafka word count");


    }
}

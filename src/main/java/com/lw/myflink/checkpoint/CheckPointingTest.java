package com.lw.myflink.checkpoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.Tuple2;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.DEFAULT_TIMEOUT;

public class CheckPointingTest {
    public static void main(String[] args) throws IOException {
        ParameterTool parameterTool=ParameterTool.fromArgs(args);
        StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();


//          environment.setStateBackend(new MemoryStateBackend());
            String statePath="hdfs://..../flink/xxx/checkpoint";
            environment.setStateBackend(new FsStateBackend(statePath));

            environment.setStateBackend(new RocksDBStateBackend(""));
//      默认不开启，开启的话迭代式数据流有可能不能运行。
        environment.enableCheckpointing(1000,CheckpointingMode.EXACTLY_ONCE); //默认500毫秒
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        //从JobManager发起一次barrier(也就是一次checkpoint）到
//        所有算子完成barrier之前数据处理和状态存储。的最长时间。
        checkpointConfig.setCheckpointTimeout(DEFAULT_TIMEOUT/2);//5分钟
        //最大并行checkpoint，前一两个还没做完，新的barrier已经emit了。
        checkpointConfig.setMaxConcurrentCheckpoints(3);
//        设置CheckPoint完成后，下一个checkPoint必须等待多久才可以
        //checkPoint花费时间过长或过于频繁可以做这个设置。
//        设置后， 并发的checkPoint就不生效了。只能串行了。
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
// cp失败 程序失败。默认true。
        checkpointConfig.setFailOnCheckpointingErrors(true);

//        重启3次 每次间隔3S
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        // 5分钟重启失败，程序挂掉。 每次失败间隔3s。
        environment.setRestartStrategy(
                RestartStrategies.
                failureRateRestart
                        (3, Time.of(5,TimeUnit.MINUTES),
                                Time.of(3000,TimeUnit.MILLISECONDS)));
        DataStreamSource<String> socketSource = environment.socketTextStream("localhost", 8088);
        socketSource.filter(e->!e.contains("$$"))
                .flatMap((e,out)->{
                    String[] split = e.split(" ");
                    for (String item:split){
                        out.collect(item);
                    }
                })
                .map(e->new Tuple2(e,1))
                .keyBy(0)
                .addSink(new FlinkKafkaProducer011
                        ("brokerList","topic",
                                new SimpleStringSchema()));
    }
}

package com.lw.myflink.accumulator;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

public class LongCounterCode {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.fromElements("a", "b", "c", "d", "e", "f");
        MapOperator<String, String> map = dataSource.map(new RichMapFunction<String, String>() {

            //1.创建累加器,在算子中创建累加器对象
            private LongCounter numLines = new LongCounter();

            //2.注册累加器对象，通常在Rich函数的open方法中使用
            // getRuntimeContext().addAccumulator("num-lines", this.numLines);注册累加器
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }

            @Override
            public String map(String s) throws Exception {
                //3.使用累加器 ，可以在任意操作中使用，包括在open或者close方法中
                this.numLines.add(1l);
                return s;
            }
        }).setParallelism(8);

        map.writeAsText("./TempResult/result",FileSystem.WriteMode.OVERWRITE);

        JobExecutionResult myJobExecutionResult = env.execute("LongCounterTest");
        //4.当作业执行完成之后，在JobExecutionResult对象中获取累加器的值。
        Long accumulatorResult = myJobExecutionResult.getAccumulatorResult("num-lines");
        System.out.println("accumulator value = "+accumulatorResult);
    }
}

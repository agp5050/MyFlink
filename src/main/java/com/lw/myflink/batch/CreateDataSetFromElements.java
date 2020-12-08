package com.lw.myflink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理中根据一些元素创建DataSet
 */

public class CreateDataSetFromElements {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataStream = env.fromElements("hello zhangsan", "hello flink", "hello java");
        FlatMapOperator<String, String> flatMap = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                for (String s : line.split(" ")) {
                    collector.collect(s);
                }
            }
        });

        UnsortedGrouping<String> groupBy = flatMap.groupBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s;
            }
        });

        GroupReduceOperator<String, Tuple2<String, Integer>> reduceGroup =
                groupBy.reduceGroup(new GroupReduceFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void reduce(Iterable<String> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                Integer count = 0;
                String key = "";
                for (String s : iterable) {
                    count++;
                    key = s;
                }
                collector.collect(new Tuple2<>(key, count));
            }
        });

        reduceGroup.print();


    }
}

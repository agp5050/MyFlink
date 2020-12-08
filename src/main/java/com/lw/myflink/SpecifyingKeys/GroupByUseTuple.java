package com.lw.myflink.SpecifyingKeys;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 批处理中定义tuple来指定key，使用groupBy:
 *  1.可以指定tuple中的哪个元素当做key
 *  2.可以指定tuple中联合的元素当做key
 *
 */
public class GroupByUseTuple {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.fromCollection(Arrays.asList("hello flink 0", "hello flink 1", "hello java 0"));
        //数据转换成Tuple3格式
        MapOperator<String, Tuple3<String, String, Integer>> map = dataSource.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String line) throws Exception {
                return new Tuple3<>(line.split(" ")[0], line.split(" ")[1], Integer.valueOf(line.split(" ")[2]));
            }
        });
        //使用tuple的第1个元素当做key
//        UnsortedGrouping<Tuple3<String, String, Integer>> group = map.groupBy(0);
        //使用tuple的第2个元素当做key
//        UnsortedGrouping<Tuple3<String, String, Integer>> group = map.groupBy(1);
        //使用tuple的第0,2个元素当做组合key
        UnsortedGrouping<Tuple3<String, String, Integer>> group = map.groupBy(0,2);
        GroupReduceOperator<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>> groupReduce = group.reduceGroup(new GroupReduceFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>() {
            @Override
            public void reduce(Iterable<Tuple3<String, String, Integer>> iterable, Collector<Tuple3<String, String, Integer>> collector) throws Exception {
                System.out.println("new key... ... ");
                String f0 = "";
                String f1 = "";
                Integer f2 = 0;
                for (Tuple3<String, String, Integer> tp3 : iterable) {
                    System.out.println(tp3);
                    f0 += tp3.f0;
                    f1 += tp3.f1;
                    f2 += tp3.f2;

                }
                collector.collect(new Tuple3<>(f0,f1,f2));
            }
        });
        groupReduce.writeAsCsv("./TempResult/result",FileSystem.WriteMode.OVERWRITE);
        env.execute("groupBy-Tuple");
    }
}

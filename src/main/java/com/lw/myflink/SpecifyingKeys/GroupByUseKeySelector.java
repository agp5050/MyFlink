package com.lw.myflink.SpecifyingKeys;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.util.Collector;

/**
 * 批处理中使用key selector来指定key，使用groupBy:
 *
 * 使用key Selector这种方式选择key，非常方便，可以从数据类型中指定想要的key.
 *
 * socket测试数据：
 *  xiaoming	18	m	2	1	2	3
 *  xiaohong	19	f	1	4	5	3
 *  xiaoli	20	m	1	7	8	9
 */
public class GroupByUseKeySelector {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("./data/studentsScoreInfo");
        UnsortedGrouping<String> groupBy = dataSource.groupBy(new KeySelector<String, Float>() {
            @Override
            public Float getKey(String line) throws Exception {
                return Float.valueOf(line.split("\t")[6]);
            }
        });

        GroupReduceOperator<String, String> reduceGroup = groupBy.reduceGroup(new GroupReduceFunction<String, String>() {
            @Override
            public void reduce(Iterable<String> iterable, Collector<String> collector) throws Exception {
                System.out.println("new  key ... ... ");
                String value = "";
                for (String s : iterable) {
                    value += s + " #";
                }
                collector.collect(value);
            }
        });

        reduceGroup.print();
    }
}

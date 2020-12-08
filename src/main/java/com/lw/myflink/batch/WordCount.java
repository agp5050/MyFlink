package com.lw.myflink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 * flink 批处理数据WordCount:
 *  运行以下代码时，可以指定以下参数:
 *      --input:指定读取的文件
 *      --output:指定将结果保存的文件
 *  如果不指定参数，默认读取./data/words 文件，并将结果直接打印到控制台
 *
 *  排序：sum.sortPartition(1, Order.DESCENDING).setParallelism(1) 不支持全局排序，只是分区排序
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        //设置 flink 执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //设置参数全局可以使用
        env.getConfig().setGlobalJobParameters(params);
        //获取输入的数据
        DataSet<String> text;
        if (params.has("input")) {
            // read the text file from given input path
            text = env.readTextFile(params.get("input"));
        } else {
            // get default test text data
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            text = env.readTextFile("./data/words");
        }

        //flatMap 将一行数据处理成多个单词，并返回tuple2格式数据（word,1）
        FlatMapOperator<String, Tuple2<String, Integer>> flatMapTransform = text.flatMap(new Tokenizer());
        //按照tuple2中的0号位置分组
        UnsortedGrouping<Tuple2<String, Integer>> groupByTransform = flatMapTransform.groupBy(0);
        //对每组中的tuple数据的1号位置进行累加处理
        AggregateOperator<Tuple2<String, Integer>> counts = groupByTransform.sum(1);

        //保存结果
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
//            counts.writeAsCsv(params.get("output"), "\n", "#");
            // execute program
            env.execute("WordCount Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }

    }


    public static final class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : line.split(" ")) {
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}

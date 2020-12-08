package com.lw.myflink.batch;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.core.fs.FileSystem;

/**
 *  案例：读取360index文件过滤www开头的网址
 *  注意：
 *      1.通过 ExecutionEnvironment.createLocalEnvironment() 方法实例化本地环境。
 *        在大多数情况下，可以调用 ExecutionEnvironment.getExecutionEnvironment() 当程序在本地启动时该方法返回一个 LocalEnvironment。
 *      2.默认情况下，启动的本地线程数与计算机的CPU个数相同,也可以通过设置 "setParallelism" 指定所需的并发度。
 *      3.可以使用 enableLogging()/disableLogging() 将本地环境日志打印到控制台。 --【目前没有测试出来】
 *      4.本地执行环境不启动任何 Web 前端来监视执行情况。
 */
public class Flink_LocalEnvironment {
    public static void main(String[] args) throws Exception {
//        LocalEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        env.getConfig().disableSysoutLogging();
//        env.getConfig().enableSysoutLogging();
        DataSource<String> ds = env.readTextFile("./data/360index");
        /**
         * 过滤出网站含有 "https://www." 或者含有 "http://www" 的 数据行
         */
        FilterOperator<String> filterLines = ds.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String line) throws Exception {
                return line.contains("https://www.") || line.contains("http://www.");
            }
        });
        DistinctOperator<String> distinct = filterLines.distinct();
        distinct.writeAsText("./data/result/urlresult",FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute();

    }
}

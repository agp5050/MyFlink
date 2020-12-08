package com.lw.scala.myflink.batch

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

/**
  *  Scala 版本的Flink WordCount
  *   注意：
  *   1.首先需要在pom.xml中导入flink-scala包
  *   2.代码中用到 ExecutionEnvironment 类型和 隐式转换 需要导入 import org.apache.flink.api.scala._，
  *     如何是处理的流式数据，用到StreamExecutionEnvironment 类型和隐式转换，需要导入import org.apache.flink.streaming.api.scala._
  *
  *   以下代码执行需要指定参数 --input 指定输入数据的文件
  *   可以指定 --output 指定输出文件的路径，并保存成csv格式数据，也可以不指定--output 直接在控制台打印输出
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)
    if (!params.has("input")) {
      println("Executing WordCount example with default input data set.")
      println("Use --input to specify file input.")
      System.exit(1)
    }
    val text = env.readTextFile(params.get("input"))

    text
      .flatMap ( _.toLowerCase.split(" ") )
      .filter ( _.nonEmpty )
      .map ((_, 1))
      .groupBy(0)
      .sum(1)

//    if (params.has("output")) {
//      counts.writeAsCsv(params.get("output"), "\n", " ")
//      env.execute("Scala WordCount Example")
//    } else {
//      println("Printing result to stdout. Use --output to specify output path.")
//      counts.print()
//    }
    env.execute("test read text.")


  }
}

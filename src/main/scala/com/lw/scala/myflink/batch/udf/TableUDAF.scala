package com.lw.scala.myflink.batch.udf

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv}
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

object TableUDAF {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv=StreamTableEnvironment.create(env)
    val descriptor = tableEnv.connect(new FileSystem().path("dd"))
      .withFormat(new OldCsv().fieldDelimiter("_"))
    descriptor.createTemporaryTable("sensor")
    val avgTemperature=new AvgTemperature
//    Table API
//    .aggregate是全局函数，不需要分组。
//    tableEnv.registerFunction("avgTemperature",avgTemperature)
    tableEnv.from("sensor")
      .aggregate(avgTemperature('temperature) as 'avgTemp)
      .select('id,'temperature,'avgTemp)
        .toRetractStream  // 每来一条数据都要进行更改，所以要用召回输出
        .print()

    //  Table API 分组聚合
    tableEnv.from("sensor")
      .groupBy('id)
      .flatAggregate(avgTemperature('temperature) as 'avgTemp)
      .select('id,'avgTemp)
      .toRetractStream.print()

  }


  case class AvgTemp(var double: Double,var int: Int)//作为Accumulator中间结果的存放
  class AvgTemperature extends TableAggregateFunction[Double,AvgTemp]{
    override def createAccumulator(): AvgTemp = new AvgTemp(0.0,0);

//    实现聚合结果的函数accumulate
    def accumulate(acc:AvgTemp,temp:Double): Unit ={
      acc.double=acc.double+temp
      acc.int=acc.int+1
    }

//    实现一个输出结果的方法：
    def emitValue(acc:AvgTemp,collector:Collector[Double]):Unit={
      collector.collect(acc.double/acc.int)

    }
  }

}

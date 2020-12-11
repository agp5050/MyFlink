package com.lw.scala.myflink.batch

import java.util.Properties

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, GroupedTable}
import org.apache.flink.table.descriptors.{Kafka, Schema, StreamTableDescriptor}

object GenericTableRoutine {
  def main(args: Array[String]): Unit = {
    //创建TableEnv
    val tableEnv:StreamTableEnvironment=StreamTableEnvironment
      .create(StreamExecutionEnvironment.getExecutionEnvironment)
// 创建对应的connector，并连接connector
    val kafka = new Kafka()
      .topic("test")
      .properties(new Properties())
//    StreamTableDescriptor  注册一张表
    tableEnv.connect(kafka)
      .createTemporaryTable("sensor")  // 注册一张表void返回值 稍后用env.from返回表
    //    StreamTableDescriptor  注册一张输出表
    val descriptor:StreamTableDescriptor = tableEnv.connect(kafka)
//    descriptor.withFormat(new OldCsv())
    descriptor
      .createTemporaryTable("output")
    descriptor.withSchema(new Schema()
    .field("id",DataTypes.STRING())
    .field("temperature",DataTypes.DOUBLE())
    .field("timestamp",DataTypes.BIGINT()))
//    descriptor.createTemporaryTable("")  和tableEnv createTemporaryTable二选一
//  通过TableApi获取结果表
    val table = tableEnv.from("sensor").select("id, temperature")

//    通过SQL获取一张结果表
    val table1 = tableEnv.sqlQuery("select * from sensor where id == 'sensor_1'")
//    将结果表写入到输出表中
   table1.insertInto("output")

    /*聚合转换*/
    val table2 = tableEnv.from("sensor")
    val table3:GroupedTable = table2.groupBy("id")
    table3 //分组后才能聚合
//    table3.select('id,'id.count )



  }

}

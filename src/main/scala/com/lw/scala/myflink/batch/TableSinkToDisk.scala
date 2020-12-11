package com.lw.scala.myflink.batch

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema, StreamTableDescriptor}

object TableSinkToDisk {

  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val tablEnv=StreamTableEnvironment.create(env)
    val descriptor:StreamTableDescriptor = tablEnv.connect(
      new FileSystem().path("D:\\study\\MyFlink\\src\\main\\resources\\sensor"))
    descriptor
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
      .field("temp",DataTypes.DOUBLE())
      .field("timestamp",DataTypes.BIGINT()))
      .withFormat(new OldCsv().lineDelimiter(" "))
      .createTemporaryTable("sensor")
      val table = tablEnv.from("sensor")
    val table1 = table.select("id,temp")
//    tablEnv.toAppendStream(table1)


    val descriptor2:StreamTableDescriptor = tablEnv.connect(
      new FileSystem().path("D:\\study\\MyFlink\\src\\main\\resources\\target"))
    descriptor2.withSchema(new Schema()
    .field("id",DataTypes.STRING())
    .field("temp",DataTypes.DOUBLE()))
      .withFormat(new OldCsv)
      .createTemporaryTable("target")
    table1.insertInto("target")
  }

}

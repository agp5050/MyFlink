package com.lw.scala.myflink.batch.udf

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv}
import org.apache.flink.table.functions.TableFunction

/**
 * Table Function UDTF输出多条记录*/
object TableFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv=StreamTableEnvironment.create(env)
    val descriptor = tableEnv.connect(new FileSystem().path("aa"))
    descriptor.withFormat(new OldCsv().fieldDelimiter(" "))
      .createTemporaryTable("sensor")
    val table = tableEnv.from("sensor")

//    Table API
    val split=new Split("_")
    table.joinLateral( "split('id) as ('word,'wlen)")
      .select('id,'ts,'word,'wlen)

// SQL API

    tableEnv.registerFunction("split",split)
    val table1 = tableEnv.sqlQuery(
      """
        |select
        | id,
        | tw,
        | word,
        | wlen
        |from
        |sensor, lateral table( split(id)) as split_id(word,wlen)
        |""".stripMargin)
    table1


  }
  class Split(delimiter:String) extends TableFunction[(String,Int)]{
    def eval(string:String):Unit={
      string.split(delimiter).foreach(
        item => collect((item,item.length))
      )
    }
  }
}

package com.lw.scala.myflink.batch

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, GroupWindowedTable, Over, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Rowtime, Schema, StreamTableDescriptor}
import org.apache.flink.types.Row

object TableWindow {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv=StreamTableEnvironment.create(env)
    val descriptor:StreamTableDescriptor = tableEnv.connect(new FileSystem() path "aaa")


    descriptor
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("temperature",DataTypes.DOUBLE())
        .field("ts",DataTypes.BIGINT())
        .rowtime(
          new Rowtime()
            .watermarksPeriodicBounded(1000)
            .timestampsFromField("ts")
        ))
      .withFormat(new OldCsv().fieldDelimiter(" "))
      .createTemporaryTable("sensor")


    val table = tableEnv.from("sensor")
    //Table GroupBy window
    val table1 :GroupWindowedTable= table.window(Tumble over 10.seconds  on 'ts as 'tw)
    val table2 = table1.groupBy('id, 'tw)
      .select('id, 'id.count, 'temperature.avg, 'tw.end)
    table2.printSchema()
    table2.toAppendStream(TypeInformation.of(classOf[Row]))
      .print()

    val sql ="""
      |
      |select
      |id,
      |count(id),
      |avg(temperature),
      |tumble_end(ts,interval '1' seconds)
      |from sensor
      |group by
      |id,
      |tumble(ts,interval '1' second)
      |""".stripMargin
    val table3 = tableEnv.sqlQuery(sql)
    table3.toAppendStream(TypeInformation.of(classOf[Row])).print()
    table3.toRetractStream(TypeInformation.of(classOf[Row])).print()

    //Table OverWindow
    val table4 = table.window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
      .select('id, 'id.count over 'ow, 'temperature.avg over 'ow)
    table4.toAppendStream(TypeInformation.of(classOf[Row]))
      .print()


//    DDL OverWindow
    val sql2:String=
      """
        |select
        |id,
        |ts,
        |count(id) over ow,
        |avg(temperature) over ow
        |from sensor
        |window ow as(
        | partition by id,
        | order by ts
        | rows between 2 preceding and current row
        |)
        |""".stripMargin
      tableEnv.sqlQuery(sql2).toAppendStream(TypeInformation.of(classOf[Row]))
      .print()


  }

}

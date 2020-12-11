package com.lw.scala.myflink.batch

import com.lw.myflink.Sensor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Rowtime, Schema, StreamTableDescriptor}
import org.apache.flink.types.Row
import sun.plugin.cache.OldCacheEntry

object TableTime {
  def main(args: Array[String]): Unit = {

//    processing Time
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val textStream:DataStream[String] = env.readTextFile("D:\\study\\MyFlink\\src\\main\\resources\\sensor")
      val value:DataStream[Sensor] = textStream.map(str => {
          val strings = str.split(" ")
          new Sensor(strings(0), strings(1).toDouble, strings(2).toLong)
      })
      value:DataStream[Sensor]
    val tableEnv=StreamTableEnvironment.create(env)
      value.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(1)) {
          override def extractTimestamp(element: Sensor): Long = element.getTimestamp*1000L
      })
      //DataStream转换的时候（只能指定字段，但是不能
      // 指定waterMark的延迟周期和提取，所以还需要手动指定下watermark）
    val table = tableEnv.fromDataStream(value,'id,'temperature,'timestamp,'pt.rowtime)
    table.printSchema()
    tableEnv.toAppendStream(table)(TypeInformation.of(classOf[Row]))
      .print()
//  DDL定义
    val sql="";
    tableEnv.sqlQuery(sql)
//  tableDescriptorSchema里面指定（指定了间隔和提取字段，相当于指定了watermark生成方式）
      val descriptor:StreamTableDescriptor = tableEnv.connect(new FileSystem().path(""))
      val schema = new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
        .field("timestamp", DataTypes.BIGINT())
        .rowtime(new Rowtime()
          .watermarksPeriodicBounded(1000)
          .timestampsFromField("timestamp")
        )
      descriptor.withSchema(schema)
        .withFormat(new OldCsv)
        .createTemporaryTable("sensor")

  }
}

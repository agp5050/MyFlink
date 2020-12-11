package com.lw.scala.myflink.batch

import java.util.Properties

import com.lw.myflink.Sensor
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.java._

object TableTest {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment
    val stream:DataStream[String] = {
      env.readTextFile("D:\\study\\MyFlink\\src\\main\\resources\\sensor")
    }
    val value1:DataStream[Sensor] = stream.map(new MapFunction[String, Sensor] {
      override def map(value: String): Sensor = {
        val strings = value.split(" ")
        new Sensor(strings(0), strings(1).toDouble, strings(2).toLong)
      }
    })
      .filter(new FilterFunction[Sensor] {
        override def filter(value: Sensor): Boolean = "sensor_1".equals(value.getId)
      })
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //Table APi进行转换
    val table:Table = tableEnv.fromDataStream(value1)

    val table1 = table.select("id, temperature")
        table1.printSchema();
      val value = tableEnv.toAppendStream(table1,classOf[Sensor])
      value.print()


    //Sql实现
     tableEnv.createTemporaryView("sensor",table)
      val sql:String="select id,temperature from sensor where id =='sensor_1'";
    val table2 = tableEnv.sqlQuery(sql)
    val value2 = tableEnv.toAppendStream(table2,classOf[Sensor])
      value2.print()
      env.execute()
//      env.addSource(new FlinkKafkaConsumer011[String]("s",new SimpleStringSchema,new Properties()))
    value1
      val str = tableEnv.explain(table2)
    println(str)//打印执行计划

  }

}

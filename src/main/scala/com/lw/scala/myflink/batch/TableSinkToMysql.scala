package com.lw.scala.myflink.batch

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.io.jdbc.{JDBCAppendTableSink, JDBCAppendTableSinkBuilder}
import org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink}
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
object TableSinkToMysql {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv=StreamTableEnvironment.create(env)
    val value = env.fromElements(1,2,3,4,5)(TypeInformation.of(classOf[Int]))
    val table = tableEnv.fromDataStream(value)
    tableEnv.toAppendStream(table)(TypeInformation.of(classOf[Int])).print()
//    tableEnv.connect()

    val builder = new JDBCAppendTableSinkBuilder()
    val sink:JDBCAppendTableSink = builder.setDBUrl("jdbc:mysql://localhost:3306/test?useSSL=false")
      .setUsername("abc")
      .setPassword("aaaa")
      .setDrivername("com.mysql.jdbc.Driver")
      .setBatchSize(1000)
      .setQuery("REPLACE INTO user(userId,name,age,sex,createTime,updateTime) values (?,?,?,?,?,?)")
      .setParameterTypes(Types.INT, Types.STRING
        , Types.INT, Types.BOOLEAN, Types.LOCAL_DATE_TIME, Types.LOCAL_DATE_TIME)
      .build()
//    sink  Sink 1
    val stream= tableEnv.toAppendStream(table)(TypeInformation.of(classOf[Row]))
    sink.consumeDataStream(stream)

//    sink2
    tableEnv.registerTableSink("result",Array("userId","name","name","age","sex","createTime"
    ,"updateTime"),
      Array(Types.INT, Types.STRING
        , Types.INT, Types.BOOLEAN, Types.LOCAL_DATE_TIME, Types.LOCAL_DATE_TIME),sink)

    tableEnv.insertInto("result",table)


//    sink3

//    val dStream:DataStream[Int] = env.fromElements(1,2,3,4)(TypeInformation.of(classOf[Int]))
//      .addSink(JdbcSink.sink(
//        "insert into books (id, title, author, price, qty) values (?,?,?,?,?)",
//        (ps, t) -> {
//          ps.setInt(1, t.id);
//          ps.setString(2, t.title);
//          ps.setString(3, t.author);
//          ps.setDouble(4, t.price);
//          ps.setInt(5, t.qty);
//        },
//        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//          .withUrl(getDbMetadata().getUrl())
//          .withDriverName(getDbMetadata().getDriverClass())
//          .build()));
//    env.execute();
val dStream:DataStream[Int] = env.fromElements(1,2,3,4)(TypeInformation.of(classOf[Int]))
          .addSink(new TwoPhaseCommitSinkFunction(){})
  }

}

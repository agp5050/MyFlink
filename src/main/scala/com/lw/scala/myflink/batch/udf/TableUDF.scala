package com.lw.scala.myflink.batch.udf

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**User defined scalar functions*/
object TableUDF {

  def main(args: Array[String]): Unit = {
      val env=StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv=StreamTableEnvironment.create(env)
    val descriptor = tableEnv.connect(new FileSystem().path("aa"))
    descriptor.withFormat(new OldCsv().fieldDelimiter(" "))
      .createTemporaryTable("sensor")



//    table API调用UDF
//    实例化自定义函数，
    val hashCode=new HashCode(3)
    val table = tableEnv.from("sensor")
    table.select('id ,hashCode('id))
// sql模式调用UDF
//    tableEnv中注册UDF
    tableEnv.registerFunction("hashCode",hashCode)
    val table1 = tableEnv.sqlQuery(
      """
        |select,
        |id,
        |hashCode(id)
        |from
        |sensor
        |""".stripMargin
    )
    table1.toAppendStream(TypeInformation.of(classOf[Row]))
      .print()


  }

  class HashCode(val factor:Int) extends ScalarFunction{
    def eval(string: String): Long ={
      (string.hashCode*factor >>> 16) | string.hashCode*factor
    }
  }
}

package com.lw.scala.myflink.batch

import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.java.{BatchTableEnvironment, StreamTableEnvironment}

object PlanerBasedTableProcess {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
// 基于老版本planner的流处理
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment = StreamTableEnvironment.create(env,settings)

//    基于新版本的planner流处理
    val settings1 = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableNewEnv = StreamTableEnvironment.create(env,settings1)

//    基于老版本的planner 批处理
    val settingOldBatch = EnvironmentSettings.newInstance()
      .inBatchMode()
      .useOldPlanner()
      .build()
    val envBatch = ExecutionEnvironment.getExecutionEnvironment
    val batch = BatchTableEnvironment.create(envBatch)

//    基于blink版本的Table批处理
    val settings2 = EnvironmentSettings.newInstance()
      .inBatchMode()
      .useBlinkPlanner()
      .build()
//
    val batchTableEnv = TableEnvironment.create(settings2)

  }

}

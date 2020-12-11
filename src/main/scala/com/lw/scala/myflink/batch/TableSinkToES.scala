package com.lw.scala.myflink.batch
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Elasticsearch, Json, Schema}
import org.apache.http.HttpHost

import scala.collection.JavaConverters._
import scala.collection.mutable
object TableSinkToES {
 val env=StreamExecutionEnvironment.getExecutionEnvironment
  val tableEnv=StreamTableEnvironment.create(env)
  tableEnv.connect(new Elasticsearch()
  .host("aaa",9200,"http")
  .version("6")
  .index("test"))
   .inUpsertMode()
   .withFormat(new Json())
   .withSchema(new Schema()
   .field("id",DataTypes.STRING())
   .field("temp",DataTypes.DOUBLE()))



// 非Table 直接用Stream
 private val value: DataStream[String] = env.readTextFile("aa")
 value.addSink(new ElasticsearchSink.Builder[String]
 (List(new HttpHost("aa",9200),new HttpHost("bb",9200)).asJava,
  new ElasticsearchSinkFunction[String] {
   override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
    import org.elasticsearch.client.Requests
    def createIndexRequest(element: String) = {
     val json = new mutable.HashMap[String,String]()
     json.put("data", element)
     Requests.indexRequest
       .index("my-index-student-0211")
       .`type`("my-type")
       .source(json)
    }

    indexer.add(createIndexRequest(element))


   }
  }).build()
 )


}

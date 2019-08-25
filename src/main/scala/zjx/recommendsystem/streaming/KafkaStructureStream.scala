package zjx.recommendsystem.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import zjx.recommendsystem.common.constant.ConfigConstant
import zjx.recommendsystem.common.transformer.KuduForeachWriter

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * 项目名称：data-application
  * 类 名 称：KafkaStructureStream
  * 类 描 述：structure streaming 处理 kafka流数据
  * 创建时间：2019/7/26 15:37
  * 创 建 人：jiaxin.zhang
  */
object KafkaStructureStream extends Logging {

  /**
    * 结构流数据写入kudu
    *
    * @param spark      spark
    * @param topic      kafka topic
    * @param pri_key    kudu对应model kudu主键
    * @param table_name kudu对应model kudu表名
    * @tparam T1 kudu入库model
    * @tparam T2 kafka 对应json解析model
    */
  def structureStream2Kudu[T1: ClassTag, T2 <: Product : TypeTag](spark: SparkSession, topic: String, pri_key: Seq[String], table_name: String): Unit = {
    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ConfigConstant.zkServers)
      .option("subscribe", topic)
      .load()

    import org.apache.spark.sql.Encoders
    val jsonSchema = Encoders.product[T2].schema
    val jsonStr = df.selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", jsonSchema) as "data")
      .select("data.*")
    logInfo("打印>>>>>>>>>>")
    jsonStr.schema.printTreeString()

    val query = jsonStr.na.drop("all", pri_key)
      .writeStream
      .foreach(new KuduForeachWriter[T1](table_name))
      .outputMode("update")
      .start()

    query.awaitTermination()
  }

}

package zjx.recommendsystem.common.util.kafkastream


import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.SerializableSerializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.{Assign, Subscribe}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import zjx.recommendsystem.common.util.ConfigUtils._

import scala.collection.JavaConverters._


/**
  * @title: SparkDirectStream
  * @projectName recommendsystem
  * @description: TODO
  * @author zjx
  * @date 19-5-5上午11:38
  */
object SparkKafkaStream extends Logging {

  def createDirectStream(streamingContext:StreamingContext,
                         topics:Array[String],
                         kafkaParams:Map[String, Object]):InputDStream[ConsumerRecord[String, String]] = {
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream
  }

  def createDirectStreamWithOffset(streamingContext:StreamingContext,
                                   topics:Array[String],
                                   kafkaParams:Map[String, Object],
                                   zkClient: ZkClient = new ZkClient(zkServers,10000,10000,SerializableSerializer),
                                   zkOffsetPath: String = zkOffsetsPath):InputDStream[ConsumerRecord[String, String]] = {
    val fromOffsetsData = KafkaOffsetManager.readOffsets(topics,zkClient,zkOffsetPath)
    val stream = fromOffsetsData match {
      case None =>
        createDirectStream(streamingContext,topics,kafkaParams)
      case Some(fromOffsets) =>
        KafkaUtils.createDirectStream[String, String](
          streamingContext,
          PreferConsistent,
          Assign[String,String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
        )
    }
    stream
  }

  def createRDDWithOffsetRanges(sparkContext:SparkContext,
                                kafkaParams:Map[String, Object],
                                offsetRanges:Array[OffsetRange]):RDD[ConsumerRecord[String, String]] = {
    val rdd = KafkaUtils.createRDD[String,String](sparkContext,kafkaParams.asJava,offsetRanges,PreferConsistent)
    rdd
  }

}

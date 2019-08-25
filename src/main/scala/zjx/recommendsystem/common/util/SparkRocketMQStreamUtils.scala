//package zjx.recommendsystem.common.util
//
//import java.util
//import java.util.Properties
//
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.api.java.{JavaInputDStream, JavaStreamingContext}
//import org.apache.spark.streaming.dstream.InputDStream
//import zjx.recommendsystem.common.constant.ConfigConstant
//
///**
//  * 项目名称：data-application
//  * 类 名 称：SparkRocketMQStreamUtils
//  * 类 描 述：spark接入rocketMQ
//  * 创建时间：2019/7/12 15:12
//  * 创 建 人：jiaxin.zhang
//  */
//object SparkRocketMQStreamUtils {
//
//  /**
//    * 创建spark-rocketMQ可信赖的push流
//    *
//    * @param jssc
//    * @param consumerGroup
//    * @param consumerTopic
//    * @param offset
//    * @param nameserverAddr
//    * @param level
//    * @return
//    */
//  def createJavaReliableMQPushStream(jssc: JavaStreamingContext, consumerGroup: String, consumerTopic: String, offset: String = RocketMQConfig.CONSUMER_OFFSET_EARLIEST, nameserverAddr: String = ConfigConstant.nameserveraddr, level: StorageLevel = StorageLevel.MEMORY_AND_DISK): JavaInputDStream[Message] = {
//    val properties = structProperties(nameserverAddr, consumerGroup, consumerTopic, offset)
//    val ds: JavaInputDStream[Message] = RocketMqUtils.createJavaReliableMQPushStream(jssc, properties, level)
//    ds
//  }
//
//  def createMQPullStream(streamingContext: StreamingContext, consumerGroup: String, consumerTopic: String, nameserverAddr: String = ConfigConstant.nameserveraddr): InputDStream[MessageExt] = {
//    val optionParams = new util.HashMap[String, String]
//    optionParams.put(RocketMQConfig.NAME_SERVER_ADDR, nameserverAddr)
//    RocketMqUtils.createMQPullStream(streamingContext, consumerGroup, consumerTopic, ConsumerStrategy.earliest, true, false, false, optionParams)
//  }
//
//  /**
//    * 结构化rocketMQ配置文件
//    *
//    * @param nameserverAddr
//    * @param consumerGroup
//    * @param consumerTopic
//    * @return
//    */
//  def structProperties(nameserverAddr: String, consumerGroup: String, consumerTopic: String, offset: String): Properties = {
//    val properties = new Properties()
//    properties.setProperty(RocketMQConfig.NAME_SERVER_ADDR, nameserverAddr)
//    properties.setProperty(RocketMQConfig.CONSUMER_GROUP, consumerGroup)
//    properties.setProperty(RocketMQConfig.CONSUMER_TOPIC, consumerTopic)
//    properties.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, offset)
//    properties
//  }
//}

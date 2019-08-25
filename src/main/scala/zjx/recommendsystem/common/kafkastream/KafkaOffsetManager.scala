package zjx.recommendsystem.common.kafkastream

import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010.OffsetRange
import zjx.recommendsystem.common.util.ZkUtils


/**
  * @title: KafkaOfsetManager
  * @projectName recommendsystem
  * @description: kafka ofset
  * @author zjx
  * @date 19-4-24下午6:12
  */
object KafkaOffsetManager extends Logging {

  def readOffsets(topics: Array[String],zkClient: ZkClient, zkOffsetPath: String): Option[Map[TopicPartition, Long]] = {
    val topicPartitionOffsets = topics.flatMap( topic => {
      val offsetsRangesStrOpt = ZkUtils.readDataMaybeNull(zkClient,s"$zkOffsetPath/$topic")._1
      offsetsRangesStrOpt.map( offsetsRangesStr => {
        offsetsRangesStr.split(",")
          .map(s => s.split(":"))//按冒号拆分每个分区和偏移量
          .map{case Array(partitionStr, offsetStr) => new TopicPartition(topic, partitionStr.toInt) -> offsetStr.toLong} //加工成最终的格式
      })
    }).flatten.toMap
    Option(topicPartitionOffsets)
  }

  def saveOffsets(zkClient: ZkClient, zkOffsetPath: String, offsetRanges:Array[OffsetRange]): Unit = {
    offsetRanges.groupBy(offsetRange => offsetRange.topic)
      .foreach( topicPartitionList => {
        //转换每个topic => OffsetRange为存储到zk时的字符串格式 :  分区序号1:偏移量1,分区序号2:偏移量2,......
        val offsetRangesStr = topicPartitionList._2.map(offsetRange => s"${offsetRange.partition}:${offsetRange.untilOffset}").mkString(",")
        logDebug(s" 保存的偏移量：${topicPartitionList._1} => $offsetRangesStr")
        //将最终的字符串结果保存到zk里面
        ZkUtils.updatePersistentPath(zkClient, s"$zkOffsetPath/${topicPartitionList._1}", offsetRangesStr)
      }
    )
  }
}

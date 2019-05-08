package zjx.recommendsystem.streaming

import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import zjx.recommendsystem.common.util.kafkastream.{KafkaOffsetManager, SparkKafkaStream}

/**
  * @title: KafkaStreamTest
  * @projectName recommendsystem
  * @description: TODO
  * @author zjx
  * @date 19-5-6上午11:05
  */
object KafkaStreamTest extends Logging {

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest", //
    "enable.auto.commit" -> (false: java.lang.Boolean) //
  )

  val topics = Array("topicA", "topicB")

  def directStream(ssc: StreamingContext): Unit = {
    val stream = SparkKafkaStream.createDirectStream(ssc, topics, kafkaParams)
    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
  }

  def directStreamWithOffset(ssc: StreamingContext): Unit = {
    //创建zkClient注意最后一个参数最好是ZKStringSerializer类型的，不然写进去zk里面的偏移量是乱码
    val zkClient = new ZkClient("192.168.10.6:2181,192.168.10.7:2181,192.168.10.8:2181",
      30000, 30000)
    val zkOffsetPath = "/sparkstreaming" //zk的路径
    val stream = SparkKafkaStream.createDirectStreamWithOffset(ssc, topics, kafkaParams, zkClient, zkOffsetPath)

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      //      val results = yourCalculation(rdd)

      // begin your transaction

      // update results
      // update offsets where the end of existing offsets matches the beginning of this batch of offsets
      KafkaOffsetManager.saveOffsets(zkClient, zkOffsetPath, offsetRanges)
      // assert that offsets were updated correctly

      // end your transaction
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true") //优雅的关闭
    conf.set("spark.streaming.backpressure.enabled", "true") //激活削峰功能
    conf.set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值
    conf.set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
    val ssc = new StreamingContext(conf, Seconds(1))
    directStream(ssc)
    directStreamWithOffset(ssc)
    //开始执行
    ssc.start()

    //启动接受停止请求的守护进程
    //    daemonHttpServer(5555,ssc)  //方式一通过Http方式优雅的关闭策略

    //stopByMarkFile(ssc)       //方式二通过扫描HDFS文件来优雅的关闭

    //等待任务终止
    ssc.awaitTermination()
  }
}

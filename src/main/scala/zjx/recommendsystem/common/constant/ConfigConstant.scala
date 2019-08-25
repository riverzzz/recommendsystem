package zjx.recommendsystem.common.constant

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging

/**
  * 项目名称：data-application
  * 类 名 称：ConfigConstant
  * 类 描 述：配置信息读取常量类
  * 创建时间：2019/7/12 15:01
  * 创 建 人：jiaxin.zhang
  */
object ConfigConstant extends Logging {
  lazy val env = System.getProperty("scala.env")
  logInfo(s"env>>>>>>>$env")
  lazy val config: Config = if (StringUtils.isNotEmpty(env)) ConfigFactory.load(s"application-$env.conf") else ConfigFactory.load()

  /** ------------ kafka ------------ **/
  lazy val METADATA_BROKER_LIST = "metadata.broker.list"
  lazy val BOOTSTRAP_SERVERS = "bootstrap.servers"
  lazy val ZOOKEEPER_OFFSETS_PATH = "zookeeper.offsets.path"
  lazy val brokerList = config.getString(METADATA_BROKER_LIST)
  lazy val zkServers = config.getString(BOOTSTRAP_SERVERS)
  lazy val zkOffsetsPath = config.getString(ZOOKEEPER_OFFSETS_PATH)

//  /** ------------ rocketMQ ------------ **/
//  lazy val nameserveraddr = config.getString(NAME_SERVER_ADDR)

  /** ------------ ecif url ------------ **/
  lazy val ECIF_URL = "ecif.url"
  lazy val ecifUrl = config.getString(ECIF_URL)

  /** ------------ hdfs url ------------ **/
  lazy val HDFS_URL_ROCKETMQ = "hdfs.url.rocketmq"
  lazy val hdfsUrlRocketMQ = config.getString(HDFS_URL_ROCKETMQ)

  /** ------------ hdfs url ------------ **/
  lazy val KUDU_MASTER = "kudu.master"
  lazy val kuduMaster = config.getString(KUDU_MASTER)
}

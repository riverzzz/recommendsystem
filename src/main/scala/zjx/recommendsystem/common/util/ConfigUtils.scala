package zjx.recommendsystem.common.util

import com.typesafe.config.{Config, ConfigFactory}

/**
  * @title: ConfigUtils
  * @projectName recommendsystem
  * @description: TODO
  * @author zjx
  * @date 19-5-6下午3:31
  */
object ConfigUtils {
  lazy val config: Config = ConfigFactory.load()

  /** ------------ kafka ------------ **/
  lazy val METADATA_BROKER_LIST = "metadata.broker.list"
  lazy val BOOTSTRAP_SERVERS = "bootstrap.servers"
  lazy val ZOOKEEPER_OFFSETS_PATH = "zookeeper.offsets.path"
  lazy val brokerList = config.getString(METADATA_BROKER_LIST)
  lazy val zkServers = config.getString(BOOTSTRAP_SERVERS)
  lazy val zkOffsetsPath = config.getString(ZOOKEEPER_OFFSETS_PATH)

}

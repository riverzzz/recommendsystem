package zjx.recommendsystem.common.util

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.apache.spark.internal.Logging
import org.apache.zookeeper.data.Stat

/**
  * @title: zkUtils
  * @projectName recommendsystem
  * @description: TODO
  * @author zjx
  * @date 19-5-6下午5:36
  */
object ZkUtils extends Logging {
  val ConsumersPath = "/consumers"
  val BrokerIdsPath = "/brokers/ids"
  val BrokerTopicsPath = "/brokers/topics"
  val TopicConfigPath = "/config/topics"
  val TopicConfigChangesPath = "/config/changes"
  val ControllerPath = "/controller"
  val ControllerEpochPath = "/controller_epoch"
  val ReassignPartitionsPath = "/admin/reassign_partitions"
  val DeleteTopicsPath = "/admin/delete_topics"
  val PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election"

  def getTopicPath(topic: String): String = {
    BrokerTopicsPath + "/" + topic
  }

  /**
    * create the parent path
    */
  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  def readDataMaybeNull(client: ZkClient, path: String): (Option[String], Stat) = {
    val stat: Stat = new Stat()
    val dataAndStat = try {
      (Some(client.readData(path, stat)), stat)
    } catch {
      case e: ZkNoNodeException =>
        (None, stat)
      case e2: Throwable => throw e2
    }
    dataAndStat
  }

  /**
    * Update the value of a persistent node with the given path and data.
    * create parrent directory if necessary. Never throw NodeExistException.
    * Return the updated path zkVersion
    */
  def updatePersistentPath(client: ZkClient, path: String, data: String) = {
    try {
      client.writeData(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        try {
          client.createPersistent(path, data)
        } catch {
          case e: ZkNodeExistsException =>
            client.writeData(path, data)
          case e2: Throwable => throw e2
        }
      }
      case e2: Throwable => throw e2
    }
  }

  //以后再改
  //  def getPartitionAssignmentForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[String, collection.Map[Int, Seq[Int]]] = {
  //    val ret = new mutable.HashMap[String, Map[Int, Seq[Int]]]()
  //    topics.foreach{ topic =>
  //      val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1
  //      val partitionMap = jsonPartitionMapOpt match {
  //        case Some(jsonPartitionMap) =>
  //          Json.parseFull(jsonPartitionMap) match {
  //            case Some(m) => m.asInstanceOf[Map[String, Any]].get("partitions") match {
  //              case Some(replicaMap) =>
  //                val m1 = replicaMap.asInstanceOf[Map[String, Seq[Int]]]
  //                m1.map(p => (p._1.toInt, p._2))
  //              case None => Map[Int, Seq[Int]]()
  //            }
  //            case None => Map[Int, Seq[Int]]()
  //          }
  //        case None => Map[Int, Seq[Int]]()
  //      }
  //      logDebug("Partition map for /brokers/topics/%s is %s".format(topic, partitionMap))
  //      ret += (topic -> partitionMap)
  //    }
  //    ret
  //  }
  //
  //  def getPartitionsForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[String, Seq[Int]] = {
  //    getPartitionAssignmentForTopics(zkClient, topics).map { topicAndPartitionMap =>
  //      val topic = topicAndPartitionMap._1
  //      val partitionMap = topicAndPartitionMap._2
  //      logDebug("partition assignment of /brokers/topics/%s is %s".format(topic, partitionMap))
  //      (topic -> partitionMap.keys.toSeq.sortWith((s,t) => s < t))
  //    }
  //  }
}

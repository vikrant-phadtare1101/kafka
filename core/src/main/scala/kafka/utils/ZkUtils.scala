/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import kafka.cluster.{Broker, Cluster}
import scala.collection._
import java.util.Properties
import org.I0Itec.zkclient.exception.{ZkNodeExistsException, ZkNoNodeException, ZkMarshallingError}

object ZkUtils extends Logging {
  val ConsumersPath = "/consumers"
  val BrokerIdsPath = "/brokers/ids"
  val BrokerTopicsPath = "/brokers/topics"

  def getTopicPath(topic: String): String ={
    BrokerTopicsPath + "/" + topic
  }

  def getTopicPartitionsPath(topic: String): String ={
    getTopicPath(topic) + "/" + "partitions"
  }

  def getTopicPartitionPath(topic: String, partitionId: String): String ={
    getTopicPartitionsPath(topic) + "/" + partitionId
  }

  def getTopicVersion(zkClient: ZkClient, topic: String): String ={
    readDataMaybeNull(zkClient, getTopicPath(topic))
  }

  def getTopicPartitionReplicasPath(topic: String, partitionId: String): String ={
    getTopicPartitionPath(topic, partitionId) + "/" + "replicas"
  }

  def getTopicPartitionInSyncPath(topic: String, partitionId: String): String ={
    getTopicPartitionPath(topic, partitionId) + "/" + "isr"
  }

  def getTopicPartitionLeaderPath(topic: String, partitionId: String): String ={
    getTopicPartitionPath(topic, partitionId) + "/" + "leader"
  }

  def getSortedBrokerList(zkClient: ZkClient): Seq[String] ={
      ZkUtils.getChildren(zkClient, ZkUtils.BrokerIdsPath).sorted
  }

  def registerBrokerInZk(zkClient: ZkClient, id: Int, host: String, creator: String, port: Int) {
    val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + id
    val broker = new Broker(id, creator, host, port)
    try {
      createEphemeralPathExpectConflict(zkClient, brokerIdPath, broker.getZKString)
    } catch {
      case e: ZkNodeExistsException =>
        throw new RuntimeException("A broker is already registered on the path " + brokerIdPath + ". This probably " +
                                   "indicates that you either have configured a brokerid that is already in use, or " +
                                   "else you have shutdown this broker and restarted it faster than the zookeeper " +
                                   "timeout so it appears to be re-registering.")
    }
    info("Registering broker " + brokerIdPath + " succeeded with " + broker)
  }

  /**
   *  make sure a persistent path exists in ZK. Create the path if not exist.
   */
  def makeSurePersistentPathExists(client: ZkClient, path: String) {
    if (!client.exists(path))
      client.createPersistent(path, true) // won't throw NoNodeException or NodeExistsException
  }

  /**
   *  create the parent path
   */
  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  /**
   * Create an ephemeral node with the given path and data. Create parents if necessary.
   */
  private def createEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.createEphemeral(path, data)
    }
    catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
    }
  }

  /**
   * Create an ephemeral node with the given path and data.
   * Throw NodeExistException if node already exists.
   */
  def createEphemeralPathExpectConflict(client: ZkClient, path: String, data: String): Unit = {
    try {
      createEphemeralPath(client, path, data)
    }
    catch {
      case e: ZkNodeExistsException => {
        // this can happen when there is connection loss; make sure the data is what we intend to write
        var storedData: String = null
        try {
          storedData = readData(client, path)
        }
        catch {
          case e1: ZkNoNodeException => // the node disappeared; treat as if node existed and let caller handles this
          case e2 => throw e2
        }
        if (storedData == null || storedData != data) {
          info("conflict in " + path + " data: " + data + " stored data: " + storedData)
          throw e
        }
        else {
          // otherwise, the creation succeeded, return normally
          info(path + " exists with value " + data + " during connection loss; this is ok")
        }
      }
      case e2 => throw e2
    }
  }

  /**
   * Create an persistent node with the given path and data. Create parents if necessary.
   */
  def createPersistentPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.createPersistent(path, data)
    }
    catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createPersistent(path, data)
      }
    }
  }

  /**
   * Update the value of a persistent node with the given path and data.
   * create parrent directory if necessary. Never throw NodeExistException.
   */
  def updatePersistentPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.writeData(path, data)
    }
    catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        try {
          client.createPersistent(path, data)
        }
        catch {
          case e: ZkNodeExistsException => client.writeData(path, data)
          case e2 => throw e2
        }
      }
      case e2 => throw e2
    }
  }

  /**
   * Update the value of a persistent node with the given path and data.
   * create parrent directory if necessary. Never throw NodeExistException.
   */
  def updateEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.writeData(path, data)
    }
    catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
      case e2 => throw e2
    }
  }

  def deletePath(client: ZkClient, path: String) {
    try {
      client.delete(path)
    }
    catch {
      case e: ZkNoNodeException =>
        // this can happen during a connection loss event, return normally
        info(path + " deleted during connection loss; this is ok")
      case e2 => throw e2
    }
  }

  def deletePathRecursive(client: ZkClient, path: String) {
    try {
      client.deleteRecursive(path)
    }
    catch {
      case e: ZkNoNodeException =>
        // this can happen during a connection loss event, return normally
        info(path + " deleted during connection loss; this is ok")
      case e2 => throw e2
    }
  }

  def readData(client: ZkClient, path: String): String = {
    client.readData(path)
  }

  def readDataMaybeNull(client: ZkClient, path: String): String = {
    client.readData(path, true)
  }

  def getChildren(client: ZkClient, path: String): Seq[String] = {
    import scala.collection.JavaConversions._
    // triggers implicit conversion from java list to scala Seq
    client.getChildren(path)
  }

  def getChildrenParentMayNotExist(client: ZkClient, path: String): Seq[String] = {
    import scala.collection.JavaConversions._
    // triggers implicit conversion from java list to scala Seq

    var ret: java.util.List[String] = null
    try {
      ret = client.getChildren(path)
    }
    catch {
      case e: ZkNoNodeException =>
        return Nil
      case e2 => throw e2
    }
    return ret
  }

  /**
   * Check if the given path exists
   */
  def pathExists(client: ZkClient, path: String): Boolean = {
    client.exists(path)
  }

  def getLastPart(path : String) : String = path.substring(path.lastIndexOf('/') + 1)

  def getCluster(zkClient: ZkClient) : Cluster = {
    val cluster = new Cluster
    val nodes = getChildrenParentMayNotExist(zkClient, BrokerIdsPath)
    for (node <- nodes) {
      val brokerZKString = readData(zkClient, BrokerIdsPath + "/" + node)
      cluster.add(Broker.createBroker(node.toInt, brokerZKString))
    }
    cluster
  }

  def getPartitionsForTopics(zkClient: ZkClient, topics: Iterator[String]): mutable.Map[String, List[String]] = {
    val ret = new mutable.HashMap[String, List[String]]()
    for (topic <- topics) {
      var partList: List[String] = Nil
      val brokers = getChildrenParentMayNotExist(zkClient, BrokerTopicsPath + "/" + topic)
      for (broker <- brokers) {
        val nParts = readData(zkClient, BrokerTopicsPath + "/" + topic + "/" + broker).toInt
        for (part <- 0 until nParts)
          partList ::= broker + "-" + part
      }
      partList = partList.sortWith((s,t) => s < t)
      ret += (topic -> partList)
    }
    ret
  }

  def setupPartition(zkClient : ZkClient, brokerId: Int, host: String, port: Int, topic: String, nParts: Int) {
    val brokerIdPath = BrokerIdsPath + "/" + brokerId
    val broker = new Broker(brokerId, brokerId.toString, host, port)
    createEphemeralPathExpectConflict(zkClient, brokerIdPath, broker.getZKString)
    val brokerPartTopicPath = BrokerTopicsPath + "/" + topic + "/" + brokerId
    createEphemeralPathExpectConflict(zkClient, brokerPartTopicPath, nParts.toString)    
  }

  def deletePartition(zkClient : ZkClient, brokerId: Int, topic: String) {
    val brokerIdPath = BrokerIdsPath + "/" + brokerId
    zkClient.delete(brokerIdPath)
    val brokerPartTopicPath = BrokerTopicsPath + "/" + topic + "/" + brokerId
    zkClient.delete(brokerPartTopicPath)
  }

  /**
   * For a given topic, this returns the sorted list of partition ids registered for this topic
   */
  def getSortedPartitionIdsForTopic(zkClient: ZkClient, topic: String): Seq[Int] = {
    val topicPartitionsPath = ZkUtils.getTopicPartitionsPath(topic)
    ZkUtils.getChildrenParentMayNotExist(zkClient, topicPartitionsPath).map(pid => pid.toInt).sortWith((s,t) => s < t)
  }

  def getBrokerInfoFromIds(zkClient: ZkClient, brokerIds: Seq[Int]): Seq[Broker] =
    brokerIds.map( bid => Broker.createBroker(bid, ZkUtils.readData(zkClient, ZkUtils.BrokerIdsPath + "/" + bid)) )
}

object ZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data : Object) : Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes : Array[Byte]) : Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}

class ZKGroupDirs(val group: String) {
  def consumerDir = ZkUtils.ConsumersPath
  def consumerGroupDir = consumerDir + "/" + group
  def consumerRegistryDir = consumerGroupDir + "/ids"
}

class ZKGroupTopicDirs(group: String, topic: String) extends ZKGroupDirs(group) {
  def consumerOffsetDir = consumerGroupDir + "/offsets/" + topic
  def consumerOwnerDir = consumerGroupDir + "/owners/" + topic
}


class ZKConfig(props: Properties) {
  /** ZK host string */
  val zkConnect = Utils.getString(props, "zk.connect", null)

  /** zookeeper session timeout */
  val zkSessionTimeoutMs = Utils.getInt(props, "zk.sessiontimeout.ms", 6000)

  /** the max time that the client waits to establish a connection to zookeeper */
  val zkConnectionTimeoutMs = Utils.getInt(props, "zk.connectiontimeout.ms",zkSessionTimeoutMs)

  /** how far a ZK follower can be behind a ZK leader */
  val zkSyncTimeMs = Utils.getInt(props, "zk.synctime.ms", 2000)
}

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

package kafka.server

import kafka.utils._
import org.apache.zookeeper.Watcher.Event.KeeperState
import kafka.log.LogManager
import java.net.InetAddress
import kafka.common.KafkaZookeeperClient
import kafka.cluster.Replica
import org.I0Itec.zkclient.{IZkDataListener, IZkChildListener, IZkStateListener, ZkClient}

/**
 * Handles the server's interaction with zookeeper. The server needs to register the following paths:
 *   /topics/[topic]/[node_id-partition_num]
 *   /brokers/[0...N] --> host:port
 *
 */
class KafkaZooKeeper(config: KafkaConfig, logManager: LogManager) extends Logging {

  val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + config.brokerId
  var zkClient: ZkClient = null
  var topics: List[String] = Nil
  val lock = new Object()
  var existingTopics: Set[String] = Set.empty[String]
  val leaderChangeListener = new LeaderChangeListener
  val topicPartitionsChangeListener = new TopicChangeListener
  private val topicListenerLock = new Object
  private val leaderChangeLock = new Object

  def startup() {
    /* start client */
    info("connecting to ZK: " + config.zkConnect)
    zkClient = KafkaZookeeperClient.getZookeeperClient(config)
    zkClient.subscribeStateChanges(new SessionExpireListener)
    subscribeToTopicAndPartitionsChanges
  }

  def registerBrokerInZk() {
    info("Registering broker " + brokerIdPath)
    val hostName = if (config.hostName == null) InetAddress.getLocalHost.getHostAddress else config.hostName
    val creatorId = hostName + "-" + System.currentTimeMillis
    ZkUtils.registerBrokerInZk(zkClient, config.brokerId, hostName, creatorId, config.port)
  }

  /**
   *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
   *  connection for us. We need to re-register this broker in the broker registry.
   */
  class SessionExpireListener() extends IZkStateListener {
    @throws(classOf[Exception])
    def handleStateChanged(state: KeeperState) {
      // do nothing, since zkclient will do reconnect for us.
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
     *
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleNewSession() {
      info("re-registering broker info in ZK for broker " + config.brokerId)
      registerBrokerInZk()
      info("done re-registering broker")
      info("Subscribing to %s path to watch for new topics".format(ZkUtils.BrokerTopicsPath))
      zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicPartitionsChangeListener)
      val topics = ZkUtils.getAllTopics(zkClient)
      debug("Existing topics are %s".format(topics.mkString(",")))
      topics.foreach(topic => zkClient.subscribeChildChanges(ZkUtils.getTopicPartitionsPath(topic), topicPartitionsChangeListener))
      handleNewTopics(topics)
    }
  }

  def close() {
    if (zkClient != null) {
      info("Closing zookeeper client...")
      zkClient.close()
    }
  }

  def handleNewTopics(topics: Seq[String]) {
    // get relevant partitions to this broker
    val topicsAndPartitionsOnThisBroker = ZkUtils.getPartitionsAssignedToBroker(zkClient, topics, config.brokerId)
    topicsAndPartitionsOnThisBroker.foreach { tp =>
      val topic = tp._1
      val partitionsAssignedToThisBroker = tp._2
      // subscribe to leader changes for these partitions
      subscribeToLeaderForPartitions(topic, partitionsAssignedToThisBroker)
      // start replicas for these partitions
      startReplicasForPartitions(topic, partitionsAssignedToThisBroker)
    }
  }

  def handleNewPartitions(topic: String, partitions: Seq[Int]) {
    info("Handling topic %s partitions %s".format(topic, partitions.mkString(",")))
    // find the partitions relevant to this broker
    val partitionsAssignedToThisBroker = ZkUtils.getPartitionsAssignedToBroker(zkClient, topic, partitions, config.brokerId)
    info("Partitions assigned to broker %d for topic %s are %s"
      .format(config.brokerId, topic, partitionsAssignedToThisBroker.mkString(",")))

    // subscribe to leader changes for these partitions
    subscribeToLeaderForPartitions(topic, partitionsAssignedToThisBroker)
    // start replicas for these partitions
    startReplicasForPartitions(topic, partitionsAssignedToThisBroker)
  }

  def subscribeToTopicAndPartitionsChanges {
    info("Subscribing to %s path to watch for new topics".format(ZkUtils.BrokerTopicsPath))
    zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicPartitionsChangeListener)
    val topics = ZkUtils.getAllTopics(zkClient)
    debug("Existing topics are %s".format(topics.mkString(",")))
    topics.foreach(topic => zkClient.subscribeChildChanges(ZkUtils.getTopicPartitionsPath(topic), topicPartitionsChangeListener))

    val partitionsAssignedToThisBroker = ZkUtils.getPartitionsAssignedToBroker(zkClient, topics, config.brokerId)
    debug("Partitions assigned to broker %d are %s".format(config.brokerId, partitionsAssignedToThisBroker.mkString(",")))
    partitionsAssignedToThisBroker.foreach { tp =>
      val topic = tp._1
      val partitions = tp._2.map(p => p.toInt)
      partitions.foreach { partition =>
          // register leader change listener
          zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderPath(topic, partition.toString), leaderChangeListener)
      }
    }
  }

  private def subscribeToLeaderForPartitions(topic: String, partitions: Seq[Int]) {
    partitions.foreach { partition =>
      info("Broker %d subscribing to leader changes for topic %s partition %d".format(config.brokerId, topic, partition))
      // register leader change listener
      zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderPath(topic, partition.toString), leaderChangeListener)
    }
  }

  def startReplicasForTopics(topics: Seq[String]) {
    val partitionsAssignedToThisBroker = ZkUtils.getPartitionsAssignedToBroker(zkClient, topics, config.brokerId)
    partitionsAssignedToThisBroker.foreach(tp => startReplicasForPartitions(tp._1, tp._2))
  }

  private def startReplicasForPartitions(topic: String, partitions: Seq[Int]) {
    partitions.foreach { partition =>
      val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, partition).map(r => r.toInt)
      if(assignedReplicas.contains(config.brokerId)) {
        val replica = logManager.addReplicaForPartition(topic, partition)
        startReplica(replica)
      } else
        warn("Ignoring partition %d of topic %s since broker %d doesn't host any replicas for it"
          .format(partition, topic, config.brokerId))
    }
  }

  private def startReplica(replica: Replica) {
    info("Starting replica for topic %s partition %d on broker %d".format(replica.topic, replica.partition.partId, replica.brokerId))
    replica.log match {
      case Some(log) =>  // log is already started
      case None =>
      // TODO: Add log recovery upto the last checkpointed HW as part of KAFKA-46
    }
    ZkUtils.getLeaderForPartition(zkClient, replica.topic, replica.partition.partId) match {
      case Some(leader) => info("Topic %s partition %d has leader %d".format(replica.topic, replica.partition.partId, leader))
      case None => // leader election
        leaderElection(replica)

    }
  }

  def leaderElection(replica: Replica) {
    info("Broker %d electing leader for topic %s partition %d".format(config.brokerId, replica.topic, replica.partition.partId))
    // read the AR list for replica.partition from ZK
    val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, replica.topic, replica.partition.partId).map(r => r.toInt)
    // TODO: read the ISR as part of KAFKA-302
    if(assignedReplicas.contains(replica.brokerId)) {
      // wait for some time if it is not the preferred replica
      try {
        if(replica.brokerId != assignedReplicas.head)
          Thread.sleep(config.preferredReplicaWaitTime)
      }catch {
        case e => // ignoring
      }
      if(ZkUtils.tryToBecomeLeaderForPartition(zkClient, replica.topic, replica.partition.partId, replica.brokerId)) {
        info("Broker %d is leader for topic %s partition %d".format(replica.brokerId, replica.topic, replica.partition.partId))
        // TODO: Become leader as part of KAFKA-302
      }
    }
  }

  class TopicChangeListener extends IZkChildListener with Logging {

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, curChilds : java.util.List[String]) {
      topicListenerLock.synchronized {
        debug("Topic/partition change listener fired for path " + parentPath)
        import scala.collection.JavaConversions._
        val currentChildren = asBuffer(curChilds)
        // check if topic has changed or a partition for an existing topic has changed
        if(parentPath == ZkUtils.BrokerTopicsPath) {
          val currentTopics = currentChildren
          debug("New topics " + currentTopics.mkString(","))
          // for each new topic [topic], watch the path /brokers/topics/[topic]/partitions
          currentTopics.foreach(topic => zkClient.subscribeChildChanges(ZkUtils.getTopicPartitionsPath(topic), this))
          handleNewTopics(currentTopics)
        }else {
          val topic = parentPath.split("/").takeRight(2).head
          debug("Partitions changed for topic %s on broker %d with new value %s"
            .format(topic, config.brokerId, currentChildren.mkString(",")))
          handleNewPartitions(topic, currentChildren.map(p => p.toInt).toSeq)
        }
      }
    }
  }

  class LeaderChangeListener extends IZkDataListener with Logging {

    @throws(classOf[Exception])
    def handleDataChange(dataPath: String, data: Object) {
      // handle leader change event for path
      val newLeader: String = data.asInstanceOf[String]
      debug("Leader change listener fired for path %s. New leader is %s".format(dataPath, newLeader))
      // TODO: update the leader in the list of replicas maintained by the log manager
    }

    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
      leaderChangeLock.synchronized {
        // leader is deleted for topic partition
        val topic = dataPath.split("/").takeRight(4).head
        val partitionId = dataPath.split("/").takeRight(2).head.toInt
        debug("Leader deleted listener fired for topic %s partition %d on broker %d"
          .format(topic, partitionId, config.brokerId))
        val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, partitionId).map(r => r.toInt)
        if(assignedReplicas.contains(config.brokerId)) {
          val replica = logManager.getReplicaForPartition(topic, partitionId)
          replica match {
            case Some(r) => leaderElection(r)
            case None =>  error("No replica exists for topic %s partition %s on broker %d"
              .format(topic, partitionId, config.brokerId))
          }
        }
      }
    }
  }
}




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

package kafka.consumer

import java.util.concurrent._
import java.util.concurrent.atomic._
import scala.collection._
import kafka.cluster._
import kafka.utils._
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import java.net.InetAddress
import org.I0Itec.zkclient.{IZkStateListener, IZkChildListener, ZkClient}
import org.apache.zookeeper.Watcher.Event.KeeperState
import kafka.api.OffsetRequest
import java.util.UUID
import kafka.serializer.Decoder
import java.lang.IllegalStateException
import kafka.utils.ZkUtils._
import kafka.common.{NoBrokersForPartitionException, ConsumerRebalanceFailedException, InvalidConfigException}

/**
 * This class handles the consumers interaction with zookeeper
 *
 * Directories:
 * 1. Consumer id registry:
 * /consumers/[group_id]/ids[consumer_id] -> topic1,...topicN
 * A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
 * and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
 * A consumer subscribes to event changes of the consumer id registry within its group.
 *
 * The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
 * ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
 * whether the creation of a sequential znode has succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 *
 * 2. Broker node registry:
 * /brokers/[0...N] --> { "host" : "host:port",
 *                        "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
 *                                    "topicN": ["partition1" ... "partitionN"] } }
 * This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
 * node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
 * is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
 * the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
 * A consumer subscribes to event changes of the broker node registry.
 *
 * 3. Partition owner registry:
 * /consumers/[group_id]/owner/[topic]/[broker_id-partition_id] --> consumer_node_id
 * This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
 * within a consumer group. The mapping is reestablished after each rebalancing.
 *
 * 4. Consumer offset tracking:
 * /consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] --> offset_counter_value
 * Each consumer tracks the offset of the latest message consumed for each partition.
 *
 */
private[kafka] object ZookeeperConsumerConnector {
  val shutdownCommand: FetchedDataChunk = new FetchedDataChunk(null, null, -1L)
}

/**
 *  JMX interface for monitoring consumer
 */
trait ZookeeperConsumerConnectorMBean {
  def getPartOwnerStats: String
  def getConsumerGroup: String
  def getOffsetLag(topic: String, brokerId: Int, partitionId: Int): Long
  def getConsumedOffset(topic: String, brokerId: Int, partitionId: Int): Long
  def getLatestOffset(topic: String, brokerId: Int, partitionId: Int): Long
}

private[kafka] class ZookeeperConsumerConnector(val config: ConsumerConfig,
                                                val enableFetcher: Boolean) // for testing only
  extends ConsumerConnector with ZookeeperConsumerConnectorMBean with Logging {

  private val isShuttingDown = new AtomicBoolean(false)
  private val rebalanceLock = new Object
  private var fetcher: Option[Fetcher] = None
  private var zkClient: ZkClient = null
  private val topicRegistry = new Pool[String, Pool[Int, PartitionTopicInfo]]
  // queues : (topic,consumerThreadId) -> queue
  private val queues = new Pool[Tuple2[String,String], BlockingQueue[FetchedDataChunk]]
  private val scheduler = new KafkaScheduler(1, "Kafka-consumer-autocommit-", false)

  connectZk()
  createFetcher()
  if (config.autoCommit) {
    info("starting auto committer every " + config.autoCommitIntervalMs + " ms")
    scheduler.scheduleWithRate(autoCommit, config.autoCommitIntervalMs, config.autoCommitIntervalMs)
  }

  def this(config: ConsumerConfig) = this(config, true)

  def createMessageStreams[T](topicCountMap: Map[String,Int], decoder: Decoder[T])
      : Map[String,List[KafkaMessageStream[T]]] = {
    consume(topicCountMap, decoder)
  }

  private def createFetcher() {
    if (enableFetcher)
      fetcher = Some(new Fetcher(config, zkClient))
  }

  private def connectZk() {
    info("Connecting to zookeeper instance at " + config.zkConnect)
    zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)
  }

  def shutdown() {
    val canShutdown = isShuttingDown.compareAndSet(false, true);
    if (canShutdown) {
      info("ZKConsumerConnector shutting down")
      try {
        scheduler.shutdownNow()
        fetcher match {
          case Some(f) => f.stopConnectionsToAllBrokers
          case None =>
        }
        sendShutdownToAllQueues()
        if (config.autoCommit)
          commitOffsets()
        if (zkClient != null) {
          zkClient.close()
          zkClient = null
        }
      } catch {
        case e =>
          fatal("error during consumer connector shutdown", e)
      }
      info("ZKConsumerConnector shut down completed")
    }
  }

  def consume[T](topicCountMap: scala.collection.Map[String,Int], decoder: Decoder[T])
      : Map[String,List[KafkaMessageStream[T]]] = {
    debug("entering consume ")
    if (topicCountMap == null)
      throw new RuntimeException("topicCountMap is null")

    val dirs = new ZKGroupDirs(config.groupId)
    var ret = new mutable.HashMap[String,List[KafkaMessageStream[T]]]

    var consumerUuid : String = null
    config.consumerId match {
      case Some(consumerId) => // for testing only
        consumerUuid = consumerId
      case None => // generate unique consumerId automatically
        val uuid = UUID.randomUUID()
        consumerUuid = "%s-%d-%s".format( InetAddress.getLocalHost.getHostName,
                                          System.currentTimeMillis,
                                          uuid.getMostSignificantBits().toHexString.substring(0,8) )
    }
    val consumerIdString = config.groupId + "_" + consumerUuid
    val topicCount = new TopicCount(consumerIdString, topicCountMap)

    // create a queue per topic per consumer thread
    val consumerThreadIdsPerTopic = topicCount.getConsumerThreadIdsPerTopic
    for ((topic, threadIdSet) <- consumerThreadIdsPerTopic) {
      var streamList: List[KafkaMessageStream[T]] = Nil
      for (threadId <- threadIdSet) {
        val stream = new LinkedBlockingQueue[FetchedDataChunk](config.maxQueuedChunks)
        queues.put((topic, threadId), stream)
        streamList ::= new KafkaMessageStream[T](topic, stream, config.consumerTimeoutMs, decoder)
      }
      ret += (topic -> streamList)
      debug("adding topic " + topic + " and stream to map..")
    }

    // listener to consumer and partition changes
    val loadBalancerListener = new ZKRebalancerListener[T](config.groupId, consumerIdString, ret)
    registerConsumerInZK(dirs, consumerIdString, topicCount)

    // register listener for session expired event
    zkClient.subscribeStateChanges(
      new ZKSessionExpireListener[T](dirs, consumerIdString, topicCount, loadBalancerListener))

    zkClient.subscribeChildChanges(dirs.consumerRegistryDir, loadBalancerListener)

    ret.foreach { topicAndStreams =>
      // register on broker partition path changes
      val partitionPath = BrokerTopicsPath + "/" + topicAndStreams._1
      zkClient.subscribeChildChanges(partitionPath, loadBalancerListener)
    }

    // explicitly trigger load balancing for this consumer
    loadBalancerListener.syncedRebalance()
    ret
  }

  // this API is used by unit tests only
  def getTopicRegistry: Pool[String, Pool[Int, PartitionTopicInfo]] = topicRegistry

  private def registerConsumerInZK(dirs: ZKGroupDirs, consumerIdString: String, topicCount: TopicCount) = {
    info("begin registering consumer " + consumerIdString + " in ZK")
    createEphemeralPathExpectConflict(zkClient, dirs.consumerRegistryDir + "/" + consumerIdString, topicCount.toJsonString)
    info("end registering consumer " + consumerIdString + " in ZK")
  }

  private def sendShutdownToAllQueues() = {
    for (queue <- queues.values) {
      debug("Clearing up queue")
      queue.clear()
      queue.put(ZookeeperConsumerConnector.shutdownCommand)
      debug("Cleared queue and sent shutdown command")
    }
  }

  def autoCommit() {
    trace("auto committing")
    try {
      commitOffsets()
    }
    catch {
      case t: Throwable =>
      // log it and let it go
        error("exception during autoCommit: ", t)
    }
  }

  def commitOffsets() {
    if (zkClient == null) {
      error("zk client is null. Cannot commit offsets")
      return
    }
    for ((topic, infos) <- topicRegistry) {
      val topicDirs = new ZKGroupTopicDirs(config.groupId, topic)
      for (info <- infos.values) {
        val newOffset = info.getConsumeOffset
        try {
          updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + info.partitionId,
            newOffset.toString)
        } catch {
          case t: Throwable =>
          // log it and let it go
            warn("exception during commitOffsets",  t)
        }
        debug("Committed offset " + newOffset + " for topic " + info)
      }
    }
  }

  // for JMX
  def getPartOwnerStats(): String = {
    val builder = new StringBuilder
    for ((topic, infos) <- topicRegistry) {
      builder.append("\n" + topic + ": [")
      val topicDirs = new ZKGroupTopicDirs(config.groupId, topic)
      for(partition <- infos.values) {
        builder.append("\n    {")
        builder.append{partition}
        builder.append(",fetch offset:" + partition.getFetchOffset)
        builder.append(",consumer offset:" + partition.getConsumeOffset)
        builder.append("}")
      }
      builder.append("\n        ]")
    }
    builder.toString
  }

  // for JMX
  def getConsumerGroup(): String = config.groupId

  def getOffsetLag(topic: String, brokerId: Int, partitionId: Int): Long =
    getLatestOffset(topic, brokerId, partitionId) - getConsumedOffset(topic, brokerId, partitionId)

  def getConsumedOffset(topic: String, brokerId: Int, partitionId: Int): Long = {
    val partitionInfos = topicRegistry.get(topic)
    if (partitionInfos != null) {
      val partitionInfo = partitionInfos.get(partitionId)
      if (partitionInfo != null)
        return partitionInfo.getConsumeOffset
    }

    //otherwise, try to get it from zookeeper
    try {
      val topicDirs = new ZKGroupTopicDirs(config.groupId, topic)
      val znode = topicDirs.consumerOffsetDir + "/" + partitionId
      val offsetString = readDataMaybeNull(zkClient, znode)
      if (offsetString != null)
        return offsetString.toLong
      else
        return -1
    }
    catch {
      case e =>
        error("error in getConsumedOffset JMX ", e)
    }
    return -2
  }

  def getLatestOffset(topic: String, brokerId: Int, partitionId: Int): Long =
    earliestOrLatestOffset(topic, brokerId, partitionId, OffsetRequest.LatestTime)

  private def earliestOrLatestOffset(topic: String, brokerId: Int, partitionId: Int, earliestOrLatest: Long): Long = {
    var simpleConsumer: SimpleConsumer = null
    var producedOffset: Long = -1L
    try {
      val cluster = getCluster(zkClient)
      val broker = cluster.getBroker(brokerId) match {
        case Some(b) => b
        case None => throw new IllegalStateException("Broker " + brokerId + " is unavailable. Cannot issue " +
          "getOffsetsBefore request")
      }
      simpleConsumer = new SimpleConsumer(broker.host, broker.port, ConsumerConfig.SocketTimeout,
                                            ConsumerConfig.SocketBufferSize)
      val offsets = simpleConsumer.getOffsetsBefore(topic, partitionId, earliestOrLatest, 1)
      producedOffset = offsets(0)
    } catch {
      case e =>
        error("error in earliestOrLatestOffset() ", e)
    }
    finally {
      if (simpleConsumer != null)
        simpleConsumer.close
    }
    producedOffset
  }

  class ZKSessionExpireListener[T](val dirs: ZKGroupDirs,
                                 val consumerIdString: String,
                                 val topicCount: TopicCount,
                                 val loadBalancerListener: ZKRebalancerListener[T])
    extends IZkStateListener {
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
      /**
       *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
       *  connection for us. We need to release the ownership of the current consumer and re-register this
       *  consumer in the consumer registry and trigger a rebalance.
       */
      info("ZK expired; release old broker parition ownership; re-register consumer " + consumerIdString)
      loadBalancerListener.resetState
      registerConsumerInZK(dirs, consumerIdString, topicCount)
      // explicitly trigger load balancing for this consumer
      loadBalancerListener.syncedRebalance

      // There is no need to resubscribe to child and state changes.
      // The child change watchers will be set inside rebalance when we read the children list.
    }

  }

  class ZKRebalancerListener[T](val group: String, val consumerIdString: String,
                                kafkaMessageStreams: Map[String,List[KafkaMessageStream[T]]])
    extends IZkChildListener {
    private val dirs = new ZKGroupDirs(group)
    private var oldPartitionsPerTopicMap: mutable.Map[String, Seq[String]] = new mutable.HashMap[String, Seq[String]]()
    private var oldConsumersPerTopicMap: mutable.Map[String,List[String]] = new mutable.HashMap[String,List[String]]()

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, curChilds : java.util.List[String]) {
      syncedRebalance
    }

    private def releasePartitionOwnership()= {
      info("Releasing partition ownership")
      for ((topic, infos) <- topicRegistry) {
        for(partition <- infos.keys) {
          val partitionOwnerPath = getConsumerPartitionOwnerPath(group, topic, partition.toString)
          deletePath(zkClient, partitionOwnerPath)
          debug("Consumer " + consumerIdString + " releasing " + partitionOwnerPath)
        }
      }
    }

    private def getRelevantTopicMap(myTopicThreadIdsMap: Map[String, Set[String]],
                                    newPartMap: Map[String, Seq[String]],
                                    oldPartMap: Map[String, Seq[String]],
                                    newConsumerMap: Map[String,List[String]],
                                    oldConsumerMap: Map[String,List[String]]): Map[String, Set[String]] = {
      var relevantTopicThreadIdsMap = new mutable.HashMap[String, Set[String]]()
      for ( (topic, consumerThreadIdSet) <- myTopicThreadIdsMap )
        if ( oldPartMap.get(topic) != newPartMap.get(topic) || oldConsumerMap.get(topic) != newConsumerMap.get(topic))
          relevantTopicThreadIdsMap += (topic -> consumerThreadIdSet)
      relevantTopicThreadIdsMap
    }

    def resetState() {
      topicRegistry.clear
      oldConsumersPerTopicMap.clear
      oldPartitionsPerTopicMap.clear
    }

    def syncedRebalance() {
      rebalanceLock synchronized {
        for (i <- 0 until config.maxRebalanceRetries) {
          info("begin rebalancing consumer " + consumerIdString + " try #" + i)
          var done = false
          val cluster = getCluster(zkClient)
          try {
            done = rebalance(cluster)
          } catch {
            case e =>
              /** occasionally, we may hit a ZK exception because the ZK state is changing while we are iterating.
               * For example, a ZK node can disappear between the time we get all children and the time we try to get
               * the value of a child. Just let this go since another rebalance will be triggered.
               **/
              info("exception during rebalance ", e)
              /* Explicitly make sure another rebalancing attempt will get triggered. */
              done = false
          }
          info("end rebalancing consumer " + consumerIdString + " try #" + i)
          if (done) {
            return
          } else {
              /* Here the cache is at a risk of being stale. To take future rebalancing decisions correctly, we should
               * clear the cache */
              info("Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered")
              oldConsumersPerTopicMap.clear()
              oldPartitionsPerTopicMap.clear()
          }
          // commit offsets
          commitOffsets()
          // stop all fetchers and clear all the queues to avoid data duplication
          closeFetchersForQueues(cluster, kafkaMessageStreams, queues.map(q => q._2))
          // release all partitions, reset state and retry
          releasePartitionOwnership()
          Thread.sleep(config.rebalanceBackoffMs)
        }
      }

      throw new ConsumerRebalanceFailedException(consumerIdString + " can't rebalance after " + config.maxRebalanceRetries +" retries")
    }

    private def rebalance(cluster: Cluster): Boolean = {
      val myTopicThreadIdsMap = getTopicCount(zkClient, group, consumerIdString).getConsumerThreadIdsPerTopic
      val consumersPerTopicMap = getConsumersPerTopic(zkClient, group)
      val partitionsPerTopicMap = getPartitionsForTopics(zkClient, myTopicThreadIdsMap.keys.iterator)
      val relevantTopicThreadIdsMap = getRelevantTopicMap(myTopicThreadIdsMap, partitionsPerTopicMap, oldPartitionsPerTopicMap, consumersPerTopicMap, oldConsumersPerTopicMap)
      if (relevantTopicThreadIdsMap.size <= 0) {
        info("Consumer %s with %s and topic partitions %s doesn't need to rebalance.".
          format(consumerIdString, consumersPerTopicMap, partitionsPerTopicMap))
        debug("Partitions per topic cache " + oldPartitionsPerTopicMap)
        debug("Consumers per topic cache " + oldConsumersPerTopicMap)
        return true
      }

      /**
       * fetchers must be stopped to avoid data duplication, since if the current
       * rebalancing attempt fails, the partitions that are released could be owned by another consumer.
       * But if we don't stop the fetchers first, this consumer would continue returning data for released
       * partitions in parallel. So, not stopping the fetchers leads to duplicate data.
       */
      closeFetchers(cluster, kafkaMessageStreams, relevantTopicThreadIdsMap)

      releasePartitionOwnership()

      var partitionOwnershipDecision = new collection.mutable.HashMap[(String, String), String]()
      for ((topic, consumerThreadIdSet) <- relevantTopicThreadIdsMap) {
        topicRegistry.remove(topic)
        topicRegistry.put(topic, new Pool[Int, PartitionTopicInfo])

        val topicDirs = new ZKGroupTopicDirs(group, topic)
        val curConsumers = consumersPerTopicMap.get(topic).get
        var curPartitions: Seq[String] = partitionsPerTopicMap.get(topic).get

        val nPartsPerConsumer = curPartitions.size / curConsumers.size
        val nConsumersWithExtraPart = curPartitions.size % curConsumers.size

        info("Consumer " + consumerIdString + " rebalancing the following partitions: " + curPartitions +
          " for topic " + topic + " with consumers: " + curConsumers)

        for (consumerThreadId <- consumerThreadIdSet) {
          val myConsumerPosition = curConsumers.findIndexOf(_ == consumerThreadId)
          assert(myConsumerPosition >= 0)
          val startPart = nPartsPerConsumer*myConsumerPosition + myConsumerPosition.min(nConsumersWithExtraPart)
          val nParts = nPartsPerConsumer + (if (myConsumerPosition + 1 > nConsumersWithExtraPart) 0 else 1)

          /**
           *   Range-partition the sorted partitions to consumers for better locality.
           *  The first few consumers pick up an extra partition, if any.
           */
          if (nParts <= 0)
            warn("No broker partitions consumed by consumer thread " + consumerThreadId + " for topic " + topic)
          else {
            for (i <- startPart until startPart + nParts) {
              val partition = curPartitions(i)
              info(consumerThreadId + " attempting to claim partition " + partition)
              val ownPartition = processPartition(topicDirs, partition, topic, consumerThreadId)
              if (!ownPartition)
                return false
              else // record the partition ownership decision
                partitionOwnershipDecision += ((topic, partition) -> consumerThreadId)
            }
          }
        }
      }

      /**
       * move the partition ownership here, since that can be used to indicate a truly successful rebalancing attempt
       * A rebalancing attempt is completed successfully only after the fetchers have been started correctly
       */
      if(reflectPartitionOwnershipDecision(partitionOwnershipDecision.toMap)) {
        info("Updating the cache")
        debug("Partitions per topic cache " + partitionsPerTopicMap)
        debug("Consumers per topic cache " + consumersPerTopicMap)
        oldPartitionsPerTopicMap = partitionsPerTopicMap
        oldConsumersPerTopicMap = consumersPerTopicMap
        updateFetcher(cluster, kafkaMessageStreams)
        true
      } else
        false
    }

    private def closeFetchersForQueues(cluster: Cluster,
                                       kafkaMessageStreams: Map[String,List[KafkaMessageStream[T]]],
                                       queuesToBeCleared: Iterable[BlockingQueue[FetchedDataChunk]]) {
      var allPartitionInfos = topicRegistry.values.map(p => p.values).flatten
      fetcher match {
        case Some(f) => f.stopConnectionsToAllBrokers
        f.clearFetcherQueues(allPartitionInfos, cluster, queuesToBeCleared, kafkaMessageStreams)
        info("Committing all offsets after clearing the fetcher queues")
        /**
        * here, we need to commit offsets before stopping the consumer from returning any more messages
        * from the current data chunk. Since partition ownership is not yet released, this commit offsets
        * call will ensure that the offsets committed now will be used by the next consumer thread owning the partition
        * for the current data chunk. Since the fetchers are already shutdown and this is the last chunk to be iterated
        * by the consumer, there will be no more messages returned by this iterator until the rebalancing finishes
        * successfully and the fetchers restart to fetch more data chunks
        **/
        commitOffsets
        case None =>
      }
    }

    private def closeFetchers(cluster: Cluster, kafkaMessageStreams: Map[String,List[KafkaMessageStream[T]]],
                              relevantTopicThreadIdsMap: Map[String, Set[String]]) {
      // only clear the fetcher queues for certain topic partitions that *might* no longer be served by this consumer
      // after this rebalancing attempt
      val queuesTobeCleared = queues.filter(q => relevantTopicThreadIdsMap.contains(q._1._1)).map(q => q._2)
      closeFetchersForQueues(cluster, kafkaMessageStreams, queuesTobeCleared)
    }

    private def updateFetcher[T](cluster: Cluster,
                                 kafkaMessageStreams: Map[String,List[KafkaMessageStream[T]]]) {
      // update partitions for fetcher
      var allPartitionInfos : List[PartitionTopicInfo] = Nil
      for (partitionInfos <- topicRegistry.values)
        for (partition <- partitionInfos.values)
          allPartitionInfos ::= partition
      info("Consumer " + consumerIdString + " selected partitions : " +
        allPartitionInfos.sortWith((s,t) => s.partitionId < t.partitionId).map(_.toString).mkString(","))

      fetcher match {
        case Some(f) =>
          f.startConnections(allPartitionInfos, cluster, kafkaMessageStreams)
        case None =>
      }
    }

    private def processPartition(topicDirs: ZKGroupTopicDirs, partition: String,
                                 topic: String, consumerThreadId: String) : Boolean = {
      val partitionOwnerPath = topicDirs.consumerOwnerDir + "/" + partition
      // check if some other consumer owns this partition at this time
      val currentPartitionOwner = readDataMaybeNull(zkClient, partitionOwnerPath)
      if(currentPartitionOwner != null) {
        if(currentPartitionOwner.equals(consumerThreadId)) {
          info(partitionOwnerPath + " exists with value " + currentPartitionOwner + " during connection loss; this is ok")
          addPartitionTopicInfo(topicDirs, partition, topic, consumerThreadId)
          true
        }
        else {
          info(partitionOwnerPath + " exists with value " + currentPartitionOwner)
          false
        }
      } else {
        addPartitionTopicInfo(topicDirs, partition, topic, consumerThreadId)
        true
      }
    }

    private def reflectPartitionOwnershipDecision(partitionOwnershipDecision: Map[(String, String), String]): Boolean = {
      val partitionOwnershipSuccessful = partitionOwnershipDecision.map { partitionOwner =>
        val topic = partitionOwner._1._1
        val partition = partitionOwner._1._2
        val consumerThreadId = partitionOwner._2
        val partitionOwnerPath = getConsumerPartitionOwnerPath(group, topic,partition)
        try {
          createEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId)
          info(consumerThreadId + " successfully owned partition " + partition + " for topic " + topic)
          true
        } catch {
          case e: ZkNodeExistsException =>
            // The node hasn't been deleted by the original owner. So wait a bit and retry.
            info("waiting for the partition ownership to be deleted: " + partition)
            false
          case e2 => throw e2
        }
      }
      val success = partitionOwnershipSuccessful.foldLeft(0)((sum, decision) => if(decision) 0 else 1)
      if(success > 0) false       /* even if one of the partition ownership attempt has failed, return false */
      else true
    }

    private def addPartitionTopicInfo(topicDirs: ZKGroupTopicDirs, partition: String,
                                      topic: String, consumerThreadId: String) {
      val partTopicInfoMap = topicRegistry.get(topic)

      // find the leader for this partition
      val leaderOpt = getLeaderForPartition(zkClient, topic, partition.toInt)
      leaderOpt match {
        case None => throw new NoBrokersForPartitionException("No leader available for partition %s on topic %s".
          format(partition, topic))
        case Some(l) => debug("Leader for partition %s for topic %s is %d".format(partition, topic, l))
      }
      val leader = leaderOpt.get

      val znode = topicDirs.consumerOffsetDir + "/" + partition
      val offsetString = readDataMaybeNull(zkClient, znode)
      // If first time starting a consumer, set the initial offset based on the config
      var offset : Long = 0L
      if (offsetString == null)
        offset = config.autoOffsetReset match {
              case OffsetRequest.SmallestTimeString =>
                  earliestOrLatestOffset(topic, leader, partition.toInt, OffsetRequest.EarliestTime)
              case OffsetRequest.LargestTimeString =>
                  earliestOrLatestOffset(topic, leader, partition.toInt, OffsetRequest.LatestTime)
              case _ =>
                  throw new InvalidConfigException("Wrong value in autoOffsetReset in ConsumerConfig")
        }
      else
        offset = offsetString.toLong

      val queue = queues.get((topic, consumerThreadId))
      val consumedOffset = new AtomicLong(offset)
      val fetchedOffset = new AtomicLong(offset)
      val partTopicInfo = new PartitionTopicInfo(topic,
                                                 leader,
                                                 partition.toInt,
                                                 queue,
                                                 consumedOffset,
                                                 fetchedOffset,
                                                 new AtomicInteger(config.fetchSize))
      partTopicInfoMap.put(partition.toInt, partTopicInfo)
      debug(partTopicInfo + " selected new offset " + offset)
    }
  }
}


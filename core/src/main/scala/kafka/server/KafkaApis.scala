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

import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.TopicPartition
import kafka.api._
import kafka.admin.AdminUtils
import kafka.common._
import kafka.controller.KafkaController
import kafka.coordinator.ConsumerCoordinator
import kafka.log._
import kafka.network._
import kafka.network.RequestChannel.Response
import org.apache.kafka.common.requests.{JoinGroupRequest, JoinGroupResponse, HeartbeatRequest, HeartbeatResponse, ResponseHeader, ResponseSend}
import kafka.utils.{ZkUtils, ZKGroupTopicDirs, SystemTime, Logging}
import scala.collection._
import org.I0Itec.zkclient.ZkClient

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel,
                val replicaManager: ReplicaManager,
                val coordinator: ConsumerCoordinator,
                val controller: KafkaController,
                val zkClient: ZkClient,
                val brokerId: Int,
                val config: KafkaConfig,
                val metadataCache: MetadataCache) extends Logging {

  this.logIdent = "[KafkaApi-%d] ".format(brokerId)

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  def handle(request: RequestChannel.Request) {
    try{
      trace("Handling request: " + request.requestObj + " from connection: " + request.connectionId)
      request.requestId match {
        case RequestKeys.ProduceKey => handleProducerRequest(request)
        case RequestKeys.FetchKey => handleFetchRequest(request)
        case RequestKeys.OffsetsKey => handleOffsetRequest(request)
        case RequestKeys.MetadataKey => handleTopicMetadataRequest(request)
        case RequestKeys.LeaderAndIsrKey => handleLeaderAndIsrRequest(request)
        case RequestKeys.StopReplicaKey => handleStopReplicaRequest(request)
        case RequestKeys.UpdateMetadataKey => handleUpdateMetadataRequest(request)
        case RequestKeys.ControlledShutdownKey => handleControlledShutdownRequest(request)
        case RequestKeys.OffsetCommitKey => handleOffsetCommitRequest(request)
        case RequestKeys.OffsetFetchKey => handleOffsetFetchRequest(request)
        case RequestKeys.ConsumerMetadataKey => handleConsumerMetadataRequest(request)
        case RequestKeys.JoinGroupKey => handleJoinGroupRequest(request)
        case RequestKeys.HeartbeatKey => handleHeartbeatRequest(request)
        case requestId => throw new KafkaException("Unknown api code " + requestId)
      }
    } catch {
      case e: Throwable =>
        if ( request.requestObj != null)
          request.requestObj.handleError(e, requestChannel, request)
        else {
          val response = request.body.getErrorResponse(request.header.apiVersion, e)
          val respHeader = new ResponseHeader(request.header.correlationId)

          /* If request doesn't have a default error response, we just close the connection.
             For example, when produce request has acks set to 0 */
          if (response == null)
            requestChannel.closeConnection(request.processor, request)
          else
            requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, respHeader, response)))
        }
        error("error when handling request %s".format(request.requestObj), e)
    } finally
      request.apiLocalCompleteTimeMs = SystemTime.milliseconds
  }

  def handleLeaderAndIsrRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val leaderAndIsrRequest = request.requestObj.asInstanceOf[LeaderAndIsrRequest]
    try {
      // call replica manager to handle updating partitions to become leader or follower
      val result = replicaManager.becomeLeaderOrFollower(leaderAndIsrRequest)
      val leaderAndIsrResponse = new LeaderAndIsrResponse(leaderAndIsrRequest.correlationId, result.responseMap, result.errorCode)
      // for each new leader or follower, call coordinator to handle
      // consumer group migration
      result.updatedLeaders.foreach { case partition =>
        if (partition.topic == ConsumerCoordinator.OffsetsTopicName)
          coordinator.handleGroupImmigration(partition.partitionId)
      }
      result.updatedFollowers.foreach { case partition =>
        partition.leaderReplicaIdOpt.foreach { leaderReplica =>
          if (partition.topic == ConsumerCoordinator.OffsetsTopicName &&
              leaderReplica == brokerId)
            coordinator.handleGroupEmigration(partition.partitionId)
        }
      }

      requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, leaderAndIsrResponse)))
    } catch {
      case e: KafkaStorageException =>
        fatal("Disk error during leadership change.", e)
        Runtime.getRuntime.halt(1)
    }
  }

  def handleStopReplicaRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val stopReplicaRequest = request.requestObj.asInstanceOf[StopReplicaRequest]
    val (response, error) = replicaManager.stopReplicas(stopReplicaRequest)
    val stopReplicaResponse = new StopReplicaResponse(stopReplicaRequest.correlationId, response.toMap, error)
    requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, stopReplicaResponse)))
    replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads()
  }

  def handleUpdateMetadataRequest(request: RequestChannel.Request) {
    val updateMetadataRequest = request.requestObj.asInstanceOf[UpdateMetadataRequest]
    replicaManager.maybeUpdateMetadataCache(updateMetadataRequest, metadataCache)

    val updateMetadataResponse = new UpdateMetadataResponse(updateMetadataRequest.correlationId)
    requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, updateMetadataResponse)))
  }

  def handleControlledShutdownRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val controlledShutdownRequest = request.requestObj.asInstanceOf[ControlledShutdownRequest]
    val partitionsRemaining = controller.shutdownBroker(controlledShutdownRequest.brokerId)
    val controlledShutdownResponse = new ControlledShutdownResponse(controlledShutdownRequest.correlationId,
      ErrorMapping.NoError, partitionsRemaining)
    requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, controlledShutdownResponse)))
  }


  /**
   * Handle an offset commit request
   */
  def handleOffsetCommitRequest(request: RequestChannel.Request) {
    val offsetCommitRequest = request.requestObj.asInstanceOf[OffsetCommitRequest]

    // filter non-exist topics
    val invalidRequestsInfo = offsetCommitRequest.requestInfo.filter { case (topicAndPartition, offsetMetadata) =>
      !metadataCache.contains(topicAndPartition.topic)
    }
    val filteredRequestInfo = (offsetCommitRequest.requestInfo -- invalidRequestsInfo.keys)

    // the callback for sending an offset commit response
    def sendResponseCallback(commitStatus: immutable.Map[TopicAndPartition, Short]) {
      commitStatus.foreach { case (topicAndPartition, errorCode) =>
        // we only print warnings for known errors here; only replica manager could see an unknown
        // exception while trying to write the offset message to the local log, and it will log
        // an error message and write the error code in this case; hence it can be ignored here
        if (errorCode != ErrorMapping.NoError && errorCode != ErrorMapping.UnknownCode) {
          debug("Offset commit request with correlation id %d from client %s on partition %s failed due to %s"
            .format(offsetCommitRequest.correlationId, offsetCommitRequest.clientId,
            topicAndPartition, ErrorMapping.exceptionNameFor(errorCode)))
        }
      }
      val combinedCommitStatus = commitStatus ++ invalidRequestsInfo.map(_._1 -> ErrorMapping.UnknownTopicOrPartitionCode)
      val response = OffsetCommitResponse(combinedCommitStatus, offsetCommitRequest.correlationId)
      requestChannel.sendResponse(new RequestChannel.Response(request, new RequestOrResponseSend(request.connectionId, response)))
    }

    if (offsetCommitRequest.versionId == 0) {
      // for version 0 always store offsets to ZK
      val responseInfo = filteredRequestInfo.map {
        case (topicAndPartition, metaAndError) => {
          val topicDirs = new ZKGroupTopicDirs(offsetCommitRequest.groupId, topicAndPartition.topic)
          try {
            if (metadataCache.getTopicMetadata(Set(topicAndPartition.topic), request.securityProtocol).size <= 0) {
              (topicAndPartition, ErrorMapping.UnknownTopicOrPartitionCode)
            } else if (metaAndError.metadata != null && metaAndError.metadata.length > config.offsetMetadataMaxSize) {
              (topicAndPartition, ErrorMapping.OffsetMetadataTooLargeCode)
            } else {
              ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" +
                topicAndPartition.partition, metaAndError.offset.toString)
              (topicAndPartition, ErrorMapping.NoError)
            }
          } catch {
            case e: Throwable => (topicAndPartition, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
          }
        }
      }

      sendResponseCallback(responseInfo)
    } else {
      // for version 1 and beyond store offsets in offset manager

      // compute the retention time based on the request version:
      // if it is v1 or not specified by user, we can use the default retention
      val offsetRetention =
        if (offsetCommitRequest.versionId <= 1 ||
          offsetCommitRequest.retentionMs == org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_RETENTION_TIME) {
          coordinator.offsetConfig.offsetsRetentionMs
        } else {
          offsetCommitRequest.retentionMs
        }

      // commit timestamp is always set to now.
      // "default" expiration timestamp is now + retention (and retention may be overridden if v2)
      // expire timestamp is computed differently for v1 and v2.
      //   - If v1 and no explicit commit timestamp is provided we use default expiration timestamp.
      //   - If v1 and explicit commit timestamp is provided we calculate retention from that explicit commit timestamp
      //   - If v2 we use the default expiration timestamp
      val currentTimestamp = SystemTime.milliseconds
      val defaultExpireTimestamp = offsetRetention + currentTimestamp

      val offsetData = filteredRequestInfo.mapValues(offsetAndMetadata =>
        offsetAndMetadata.copy(
          commitTimestamp = currentTimestamp,
          expireTimestamp = {
            if (offsetAndMetadata.commitTimestamp == org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP)
              defaultExpireTimestamp
            else
              offsetRetention + offsetAndMetadata.commitTimestamp
          }
        )
      )

      // call coordinator to handle commit offset
      coordinator.handleCommitOffsets(
        offsetCommitRequest.groupId,
        offsetCommitRequest.consumerId,
        offsetCommitRequest.groupGenerationId,
        offsetData,
        sendResponseCallback)
    }
  }

  /**
   * Handle a produce request
   */
  def handleProducerRequest(request: RequestChannel.Request) {
    val produceRequest = request.requestObj.asInstanceOf[ProducerRequest]

    // the callback for sending a produce response
    def sendResponseCallback(responseStatus: Map[TopicAndPartition, ProducerResponseStatus]) {
      var errorInResponse = false
      responseStatus.foreach { case (topicAndPartition, status) =>
        // we only print warnings for known errors here; if it is unknown, it will cause
        // an error message in the replica manager
        if (status.error != ErrorMapping.NoError && status.error != ErrorMapping.UnknownCode) {
          debug("Produce request with correlation id %d from client %s on partition %s failed due to %s"
            .format(produceRequest.correlationId, produceRequest.clientId,
            topicAndPartition, ErrorMapping.exceptionNameFor(status.error)))
          errorInResponse = true
        }
      }

      if (produceRequest.requiredAcks == 0) {
        // no operation needed if producer request.required.acks = 0; however, if there is any error in handling
        // the request, since no response is expected by the producer, the server will close socket server so that
        // the producer client will know that some error has happened and will refresh its metadata
        if (errorInResponse) {
          info("Close connection due to error handling produce request with correlation id %d from client id %s with ack=0"
                  .format(produceRequest.correlationId, produceRequest.clientId))
          requestChannel.closeConnection(request.processor, request)
        } else {
          requestChannel.noOperation(request.processor, request)
        }
      } else {
        val response = ProducerResponse(produceRequest.correlationId, responseStatus)
        requestChannel.sendResponse(new RequestChannel.Response(request, new RequestOrResponseSend(request.connectionId, response)))
      }
    }

    // only allow appending to internal topic partitions
    // if the client is not from admin
    val internalTopicsAllowed = produceRequest.clientId == AdminUtils.AdminClientId

    // call the replica manager to append messages to the replicas
    replicaManager.appendMessages(
      produceRequest.ackTimeoutMs.toLong,
      produceRequest.requiredAcks,
      internalTopicsAllowed,
      produceRequest.data,
      sendResponseCallback)

    // if the request is put into the purgatory, it will have a held reference
    // and hence cannot be garbage collected; hence we clear its data here in
    // order to let GC re-claim its memory since it is already appended to log
    produceRequest.emptyData()
  }

  /**
   * Handle a fetch request
   */
  def handleFetchRequest(request: RequestChannel.Request) {
    val fetchRequest = request.requestObj.asInstanceOf[FetchRequest]

    // the callback for sending a fetch response
    def sendResponseCallback(responsePartitionData: Map[TopicAndPartition, FetchResponsePartitionData]) {
      responsePartitionData.foreach { case (topicAndPartition, data) =>
        // we only print warnings for known errors here; if it is unknown, it will cause
        // an error message in the replica manager already and hence can be ignored here
        if (data.error != ErrorMapping.NoError && data.error != ErrorMapping.UnknownCode) {
          debug("Fetch request with correlation id %d from client %s on partition %s failed due to %s"
            .format(fetchRequest.correlationId, fetchRequest.clientId,
            topicAndPartition, ErrorMapping.exceptionNameFor(data.error)))
        }

        // record the bytes out metrics only when the response is being sent
        BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesOutRate.mark(data.messages.sizeInBytes)
        BrokerTopicStats.getBrokerAllTopicsStats().bytesOutRate.mark(data.messages.sizeInBytes)
      }

      val response = FetchResponse(fetchRequest.correlationId, responsePartitionData)
      requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(request.connectionId, response)))
    }

    // call the replica manager to fetch messages from the local replica
    replicaManager.fetchMessages(
      fetchRequest.maxWait.toLong,
      fetchRequest.replicaId,
      fetchRequest.minBytes,
      fetchRequest.requestInfo,
      sendResponseCallback)
  }

  /**
   * Handle an offset request
   */
  def handleOffsetRequest(request: RequestChannel.Request) {
    val offsetRequest = request.requestObj.asInstanceOf[OffsetRequest]
    val responseMap = offsetRequest.requestInfo.map(elem => {
      val (topicAndPartition, partitionOffsetRequestInfo) = elem
      try {
        // ensure leader exists
        val localReplica = if(!offsetRequest.isFromDebuggingClient)
          replicaManager.getLeaderReplicaIfLocal(topicAndPartition.topic, topicAndPartition.partition)
        else
          replicaManager.getReplicaOrException(topicAndPartition.topic, topicAndPartition.partition)
        val offsets = {
          val allOffsets = fetchOffsets(replicaManager.logManager,
                                        topicAndPartition,
                                        partitionOffsetRequestInfo.time,
                                        partitionOffsetRequestInfo.maxNumOffsets)
          if (!offsetRequest.isFromOrdinaryClient) {
            allOffsets
          } else {
            val hw = localReplica.highWatermark.messageOffset
            if (allOffsets.exists(_ > hw))
              hw +: allOffsets.dropWhile(_ > hw)
            else
              allOffsets
          }
        }
        (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.NoError, offsets))
      } catch {
        // NOTE: UnknownTopicOrPartitionException and NotLeaderForPartitionException are special cased since these error messages
        // are typically transient and there is no value in logging the entire stack trace for the same
        case utpe: UnknownTopicOrPartitionException =>
          debug("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
               offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition, utpe.getMessage))
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(utpe.getClass.asInstanceOf[Class[Throwable]]), Nil) )
        case nle: NotLeaderForPartitionException =>
          debug("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
               offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition,nle.getMessage))
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(nle.getClass.asInstanceOf[Class[Throwable]]), Nil) )
        case e: Throwable =>
          error("Error while responding to offset request", e)
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]), Nil) )
      }
    })
    val response = OffsetResponse(offsetRequest.correlationId, responseMap)
    requestChannel.sendResponse(new RequestChannel.Response(request, new RequestOrResponseSend(request.connectionId, response)))
  }

  def fetchOffsets(logManager: LogManager, topicAndPartition: TopicAndPartition, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    logManager.getLog(topicAndPartition) match {
      case Some(log) =>
        fetchOffsetsBefore(log, timestamp, maxNumOffsets)
      case None =>
        if (timestamp == OffsetRequest.LatestTime || timestamp == OffsetRequest.EarliestTime)
          Seq(0L)
        else
          Nil
    }
  }

  private def fetchOffsetsBefore(log: Log, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    val segsArray = log.logSegments.toArray
    var offsetTimeArray: Array[(Long, Long)] = null
    if(segsArray.last.size > 0)
      offsetTimeArray = new Array[(Long, Long)](segsArray.length + 1)
    else
      offsetTimeArray = new Array[(Long, Long)](segsArray.length)

    for(i <- 0 until segsArray.length)
      offsetTimeArray(i) = (segsArray(i).baseOffset, segsArray(i).lastModified)
    if(segsArray.last.size > 0)
      offsetTimeArray(segsArray.length) = (log.logEndOffset, SystemTime.milliseconds)

    var startIndex = -1
    timestamp match {
      case OffsetRequest.LatestTime =>
        startIndex = offsetTimeArray.length - 1
      case OffsetRequest.EarliestTime =>
        startIndex = 0
      case _ =>
        var isFound = false
        debug("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d".format(o._1, o._2)))
        startIndex = offsetTimeArray.length - 1
        while (startIndex >= 0 && !isFound) {
          if (offsetTimeArray(startIndex)._2 <= timestamp)
            isFound = true
          else
            startIndex -=1
        }
    }

    val retSize = maxNumOffsets.min(startIndex + 1)
    val ret = new Array[Long](retSize)
    for(j <- 0 until retSize) {
      ret(j) = offsetTimeArray(startIndex)._1
      startIndex -= 1
    }
    // ensure that the returned seq is in descending order of offsets
    ret.toSeq.sortBy(- _)
  }

  private def getTopicMetadata(topics: Set[String], securityProtocol: SecurityProtocol): Seq[TopicMetadata] = {
    val topicResponses = metadataCache.getTopicMetadata(topics, securityProtocol)
    if (topics.size > 0 && topicResponses.size != topics.size) {
      val nonExistentTopics = topics -- topicResponses.map(_.topic).toSet
      val responsesForNonExistentTopics = nonExistentTopics.map { topic =>
        if (topic == ConsumerCoordinator.OffsetsTopicName || config.autoCreateTopicsEnable) {
          try {
            if (topic == ConsumerCoordinator.OffsetsTopicName) {
              val aliveBrokers = metadataCache.getAliveBrokers
              val offsetsTopicReplicationFactor =
                if (aliveBrokers.length > 0)
                  Math.min(config.offsetsTopicReplicationFactor.toInt, aliveBrokers.length)
                else
                  config.offsetsTopicReplicationFactor.toInt
              AdminUtils.createTopic(zkClient, topic, config.offsetsTopicPartitions,
                                     offsetsTopicReplicationFactor,
                                     coordinator.offsetsTopicConfigs)
              info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                .format(topic, config.offsetsTopicPartitions, offsetsTopicReplicationFactor))
            }
            else {
              AdminUtils.createTopic(zkClient, topic, config.numPartitions, config.defaultReplicationFactor)
              info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                   .format(topic, config.numPartitions, config.defaultReplicationFactor))
            }
            new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.LeaderNotAvailableCode)
          } catch {
            case e: TopicExistsException => // let it go, possibly another broker created this topic
              new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.LeaderNotAvailableCode)
            case itex: InvalidTopicException =>
              new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.InvalidTopicCode)
          }
        } else {
          new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.UnknownTopicOrPartitionCode)
        }
      }
      topicResponses.appendAll(responsesForNonExistentTopics)
    }
    topicResponses
  }

  /**
   * Handle a topic metadata request
   */
  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    val metadataRequest = request.requestObj.asInstanceOf[TopicMetadataRequest]
    val topicMetadata = getTopicMetadata(metadataRequest.topics.toSet, request.securityProtocol)
    val brokers = metadataCache.getAliveBrokers
    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(topicMetadata.mkString(","), brokers.mkString(","), metadataRequest.correlationId, metadataRequest.clientId))
    val response = new TopicMetadataResponse(brokers.map(_.getBrokerEndPoint(request.securityProtocol)), topicMetadata, metadataRequest.correlationId)
    requestChannel.sendResponse(new RequestChannel.Response(request, new RequestOrResponseSend(request.connectionId, response)))
  }

  /*
   * Handle an offset fetch request
   */
  def handleOffsetFetchRequest(request: RequestChannel.Request) {
    val offsetFetchRequest = request.requestObj.asInstanceOf[OffsetFetchRequest]

    val response = if (offsetFetchRequest.versionId == 0) {
      // version 0 reads offsets from ZK
      val responseInfo = offsetFetchRequest.requestInfo.map( topicAndPartition => {
        val topicDirs = new ZKGroupTopicDirs(offsetFetchRequest.groupId, topicAndPartition.topic)
        try {
          if (metadataCache.getTopicMetadata(Set(topicAndPartition.topic), request.securityProtocol).size <= 0) {
            (topicAndPartition, OffsetMetadataAndError.UnknownTopicOrPartition)
          } else {
            val payloadOpt = ZkUtils.readDataMaybeNull(zkClient, topicDirs.consumerOffsetDir + "/" + topicAndPartition.partition)._1
            payloadOpt match {
              case Some(payload) => (topicAndPartition, OffsetMetadataAndError(payload.toLong))
              case None => (topicAndPartition, OffsetMetadataAndError.UnknownTopicOrPartition)
            }
          }
        } catch {
          case e: Throwable =>
            (topicAndPartition, OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata,
              ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]])))
        }
      })

      OffsetFetchResponse(collection.immutable.Map(responseInfo: _*), offsetFetchRequest.correlationId)
    } else {
      // version 1 reads offsets from Kafka;
      val offsets = coordinator.handleFetchOffsets(offsetFetchRequest.groupId, offsetFetchRequest.requestInfo).toMap

      // Note that we do not need to filter the partitions in the
      // metadata cache as the topic partitions will be filtered
      // in coordinator's offset manager through the offset cache
      OffsetFetchResponse(offsets, offsetFetchRequest.correlationId)
    }

    trace("Sending offset fetch response %s for correlation id %d to client %s."
          .format(response, offsetFetchRequest.correlationId, offsetFetchRequest.clientId))

    requestChannel.sendResponse(new RequestChannel.Response(request, new RequestOrResponseSend(request.connectionId, response)))
  }

  /*
   * Handle a consumer metadata request
   */
  def handleConsumerMetadataRequest(request: RequestChannel.Request) {
    val consumerMetadataRequest = request.requestObj.asInstanceOf[ConsumerMetadataRequest]

    val partition = coordinator.partitionFor(consumerMetadataRequest.group)

    // get metadata (and create the topic if necessary)
    val offsetsTopicMetadata = getTopicMetadata(Set(ConsumerCoordinator.OffsetsTopicName), request.securityProtocol).head

    val errorResponse = ConsumerMetadataResponse(None, ErrorMapping.ConsumerCoordinatorNotAvailableCode, consumerMetadataRequest.correlationId)

    val response =
      offsetsTopicMetadata.partitionsMetadata.find(_.partitionId == partition).map { partitionMetadata =>
        partitionMetadata.leader.map { leader =>
          ConsumerMetadataResponse(Some(leader), ErrorMapping.NoError, consumerMetadataRequest.correlationId)
        }.getOrElse(errorResponse)
      }.getOrElse(errorResponse)

    trace("Sending consumer metadata %s for correlation id %d to client %s."
          .format(response, consumerMetadataRequest.correlationId, consumerMetadataRequest.clientId))
    requestChannel.sendResponse(new RequestChannel.Response(request, new RequestOrResponseSend(request.connectionId, response)))
  }

  def handleJoinGroupRequest(request: RequestChannel.Request) {
    import JavaConversions._

    val joinGroupRequest = request.body.asInstanceOf[JoinGroupRequest]
    val respHeader = new ResponseHeader(request.header.correlationId)

    // the callback for sending a join-group response
    def sendResponseCallback(partitions: Set[TopicAndPartition], consumerId: String, generationId: Int, errorCode: Short) {
      val partitionList = partitions.map(tp => new TopicPartition(tp.topic, tp.partition)).toBuffer
      val responseBody = new JoinGroupResponse(errorCode, generationId, consumerId, partitionList)
      trace("Sending join group response %s for correlation id %d to client %s."
              .format(responseBody, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, respHeader, responseBody)))
    }

    // let the coordinator to handle join-group
    coordinator.handleJoinGroup(
      joinGroupRequest.groupId(),
      joinGroupRequest.consumerId(),
      joinGroupRequest.topics().toSet,
      joinGroupRequest.sessionTimeout(),
      joinGroupRequest.strategy(),
      sendResponseCallback)
  }

  def handleHeartbeatRequest(request: RequestChannel.Request) {
    val heartbeatRequest = request.body.asInstanceOf[HeartbeatRequest]
    val respHeader = new ResponseHeader(request.header.correlationId)

    // the callback for sending a heartbeat response
    def sendResponseCallback(errorCode: Short) {
      val response = new HeartbeatResponse(errorCode)
      trace("Sending heartbeat response %s for correlation id %d to client %s."
              .format(response, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, respHeader, response)))
    }

    // let the coordinator to handle heartbeat
    coordinator.handleHeartbeat(
      heartbeatRequest.groupId(),
      heartbeatRequest.consumerId(),
      heartbeatRequest.groupGenerationId(),
      sendResponseCallback)
  }

  def close() {
    // TODO currently closing the API is an no-op since the API no longer maintain any modules
    // maybe removing the closing call in the end when KafkaAPI becomes a pure stateless layer
    debug("Shut down complete.")
  }
}

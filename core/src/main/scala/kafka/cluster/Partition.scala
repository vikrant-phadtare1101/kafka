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
package kafka.cluster

import kafka.common._
import kafka.admin.AdminUtils
import kafka.utils.{ZkUtils, ReplicationUtils, Pool, Time, Logging}
import kafka.utils.Utils.inLock
import kafka.api.{PartitionStateInfo, LeaderAndIsr}
import kafka.log.LogConfig
import kafka.server.{OffsetManager, ReplicaManager}
import kafka.metrics.KafkaMetricsGroup
import kafka.controller.KafkaController
import kafka.message.ByteBufferMessageSet

import java.io.IOException
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.Some
import scala.collection._

import com.yammer.metrics.core.Gauge


/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
class Partition(val topic: String,
                val partitionId: Int,
                var replicationFactor: Int,
                time: Time,
                val replicaManager: ReplicaManager) extends Logging with KafkaMetricsGroup {
  private val localBrokerId = replicaManager.config.brokerId
  private val logManager = replicaManager.logManager
  private val zkClient = replicaManager.zkClient
  var leaderReplicaIdOpt: Option[Int] = None
  var inSyncReplicas: Set[Replica] = Set.empty[Replica]
  private val assignedReplicaMap = new Pool[Int,Replica]
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock()
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = "Partition [%s,%d] on broker %d: ".format(topic, partitionId, localBrokerId)
  private val stateChangeLogger = KafkaController.stateChangeLogger

  private def isReplicaLocal(replicaId: Int) : Boolean = (replicaId == localBrokerId)

  newGauge(
    topic + "-" + partitionId + "-UnderReplicated",
    new Gauge[Int] {
      def value = {
        if (isUnderReplicated) 1 else 0
      }
    }
  )

  def isUnderReplicated(): Boolean = {
    inLock(leaderIsrUpdateLock.readLock()) {
      leaderReplicaIfLocal() match {
        case Some(_) =>
          inSyncReplicas.size < assignedReplicas.size
        case None =>
          false
      }
    }
  }

  def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
    val replicaOpt = getReplica(replicaId)
    replicaOpt match {
      case Some(replica) => replica
      case None =>
        if (isReplicaLocal(replicaId)) {
          val config = LogConfig.fromProps(logManager.defaultConfig.toProps, AdminUtils.fetchTopicConfig(zkClient, topic))
          val log = logManager.createLog(TopicAndPartition(topic, partitionId), config)
          val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParentFile.getAbsolutePath)
          val offsetMap = checkpoint.read
          if (!offsetMap.contains(TopicAndPartition(topic, partitionId)))
            warn("No checkpointed highwatermark is found for partition [%s,%d]".format(topic, partitionId))
          val offset = offsetMap.getOrElse(TopicAndPartition(topic, partitionId), 0L).min(log.logEndOffset)
          val localReplica = new Replica(replicaId, this, time, offset, Some(log))
          addReplicaIfNotExists(localReplica)
        } else {
          val remoteReplica = new Replica(replicaId, this, time)
          addReplicaIfNotExists(remoteReplica)
        }
        getReplica(replicaId).get
    }
  }

  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = {
    val replica = assignedReplicaMap.get(replicaId)
    if (replica == null)
      None
    else
      Some(replica)
  }

  def leaderReplicaIfLocal(): Option[Replica] = {
    inLock(leaderIsrUpdateLock.readLock()) {
      leaderReplicaIdOpt match {
        case Some(leaderReplicaId) =>
          if (leaderReplicaId == localBrokerId)
            getReplica(localBrokerId)
          else
            None
        case None => None
      }
    }
  }

  def addReplicaIfNotExists(replica: Replica) = {
    assignedReplicaMap.putIfNotExists(replica.brokerId, replica)
  }

  def assignedReplicas(): Set[Replica] = {
    assignedReplicaMap.values.toSet
  }

  def removeReplica(replicaId: Int) {
    assignedReplicaMap.remove(replicaId)
  }

  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inLock(leaderIsrUpdateLock.writeLock()) {
      assignedReplicaMap.clear()
      inSyncReplicas = Set.empty[Replica]
      leaderReplicaIdOpt = None
      try {
        logManager.deleteLog(TopicAndPartition(topic, partitionId))
      } catch {
        case e: IOException =>
          fatal("Error deleting the log for partition [%s,%d]".format(topic, partitionId), e)
          Runtime.getRuntime().halt(1)
      }
    }
  }

  def getLeaderEpoch(): Int = {
    inLock(leaderIsrUpdateLock.readLock()) {
      return this.leaderEpoch
    }
  }

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset from the time when this broker was the leader last time)
   *  and setting the new leader and ISR
   */
  def makeLeader(controllerId: Int,
                 partitionStateInfo: PartitionStateInfo, correlationId: Int,
                 offsetManager: OffsetManager): Boolean = {
    inLock(leaderIsrUpdateLock.writeLock()) {
      val allReplicas = partitionStateInfo.allReplicas
      val leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
      // add replicas that are new
      allReplicas.foreach(replica => getOrCreateReplica(replica))
      val newInSyncReplicas = leaderAndIsr.isr.map(r => getOrCreateReplica(r)).toSet
      // remove assigned replicas that have been removed by the controller
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      // reset LogEndOffset for remote replicas
      assignedReplicas.foreach(r => if (r.brokerId != localBrokerId) r.logEndOffset = ReplicaManager.UnknownLogEndOffset)
      inSyncReplicas = newInSyncReplicas
      leaderEpoch = leaderAndIsr.leaderEpoch
      zkVersion = leaderAndIsr.zkVersion
      leaderReplicaIdOpt = Some(localBrokerId)
      // we may need to increment high watermark since ISR could be down to 1
      maybeIncrementLeaderHW(getReplica().get)
      if (topic == OffsetManager.OffsetsTopicName)
        offsetManager.loadOffsetsFromLog(partitionId)
      true
    }
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change, return false to indicate the replica manager
   */
  def makeFollower(controllerId: Int,
                   partitionStateInfo: PartitionStateInfo,
                   correlationId: Int, offsetManager: OffsetManager): Boolean = {
    inLock(leaderIsrUpdateLock.writeLock()) {
      val allReplicas = partitionStateInfo.allReplicas
      val leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
      val newLeaderBrokerId: Int = leaderAndIsr.leader
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
      // add replicas that are new
      allReplicas.foreach(r => getOrCreateReplica(r))
      // remove assigned replicas that have been removed by the controller
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      inSyncReplicas = Set.empty[Replica]
      leaderEpoch = leaderAndIsr.leaderEpoch
      zkVersion = leaderAndIsr.zkVersion

      leaderReplicaIdOpt.foreach { leaderReplica =>
        if (topic == OffsetManager.OffsetsTopicName &&
           /* if we are making a leader->follower transition */
           leaderReplica == localBrokerId)
          offsetManager.clearOffsetsInPartition(partitionId)
      }

      if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == newLeaderBrokerId) {
        false
      }
      else {
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  def updateLeaderHWAndMaybeExpandIsr(replicaId: Int, offset: Long) {
    inLock(leaderIsrUpdateLock.writeLock()) {
      debug("Recording follower %d position %d for partition [%s,%d].".format(replicaId, offset, topic, partitionId))
      val replicaOpt = getReplica(replicaId)
      if(!replicaOpt.isDefined) {
        throw new NotAssignedReplicaException(("Leader %d failed to record follower %d's position %d for partition [%s,%d] since the replica %d" +
          " is not recognized to be one of the assigned replicas %s for partition [%s,%d]").format(localBrokerId, replicaId,
            offset, topic, partitionId, replicaId, assignedReplicas().map(_.brokerId).mkString(","), topic, partitionId))
      }
      val replica = replicaOpt.get
      replica.logEndOffset = offset

      // check if this replica needs to be added to the ISR
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          val replica = getReplica(replicaId).get
          val leaderHW = leaderReplica.highWatermark
          // For a replica to get added back to ISR, it has to satisfy 3 conditions-
          // 1. It is not already in the ISR
          // 2. It is part of the assigned replica list. See KAFKA-1097
          // 3. It's log end offset >= leader's highwatermark
          if (!inSyncReplicas.contains(replica) && assignedReplicas.map(_.brokerId).contains(replicaId) && replica.logEndOffset >= leaderHW) {
            // expand ISR
            val newInSyncReplicas = inSyncReplicas + replica
            info("Expanding ISR for partition [%s,%d] from %s to %s"
                 .format(topic, partitionId, inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in ZK and cache
            val (updateSucceeded,newVersion) = ReplicationUtils.updateIsr(zkClient, topic, partitionId, localBrokerId,
              leaderEpoch, controllerEpoch, zkVersion, newInSyncReplicas)
            if(updateSucceeded) {
              inSyncReplicas = newInSyncReplicas
              zkVersion = newVersion
              trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newInSyncReplicas.mkString(","), zkVersion))
            } else {
              info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
            }
            replicaManager.isrExpandRate.mark()
          }
          maybeIncrementLeaderHW(leaderReplica)
        case None => // nothing to do if no longer leader
      }
    }
  }

  def checkEnoughReplicasReachOffset(requiredOffset: Long, requiredAcks: Int): (Boolean, Short) = {
    inLock(leaderIsrUpdateLock.readLock()) {
      leaderReplicaIfLocal() match {
        case Some(_) =>
          val numAcks = inSyncReplicas.count(r => {
            if (!r.isLocal)
              r.logEndOffset >= requiredOffset
            else
              true /* also count the local (leader) replica */
          })
          trace("%d/%d acks satisfied for %s-%d".format(numAcks, requiredAcks, topic, partitionId))
          if ((requiredAcks < 0 && numAcks >= inSyncReplicas.size) ||
            (requiredAcks > 0 && numAcks >= requiredAcks)) {
            /*
            * requiredAcks < 0 means acknowledge after all replicas in ISR
            * are fully caught up to the (local) leader's offset
            * corresponding to this produce request.
            */
            (true, ErrorMapping.NoError)
          } else
            (false, ErrorMapping.NoError)
        case None =>
          (false, ErrorMapping.NotLeaderForPartitionCode)
      }
    }
  }

  /**
   * There is no need to acquire the leaderIsrUpdate lock here since all callers of this private API acquire that lock
   * @param leaderReplica
   */
  private def maybeIncrementLeaderHW(leaderReplica: Replica) {
    val allLogEndOffsets = inSyncReplicas.map(_.logEndOffset)
    val newHighWatermark = allLogEndOffsets.min
    val oldHighWatermark = leaderReplica.highWatermark
    if(newHighWatermark > oldHighWatermark) {
      leaderReplica.highWatermark = newHighWatermark
      debug("Highwatermark for partition [%s,%d] updated to %d".format(topic, partitionId, newHighWatermark))
    }
    else
      debug("Old hw for partition [%s,%d] is %d. New hw is %d. All leo's are %s"
        .format(topic, partitionId, oldHighWatermark, newHighWatermark, allLogEndOffsets.mkString(",")))
  }

  def maybeShrinkIsr(replicaMaxLagTimeMs: Long,  replicaMaxLagMessages: Long) {
    inLock(leaderIsrUpdateLock.writeLock()) {
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs, replicaMaxLagMessages)
          if(outOfSyncReplicas.size > 0) {
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.size > 0)
            info("Shrinking ISR for partition [%s,%d] from %s to %s".format(topic, partitionId,
              inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in zk and in cache
            val (updateSucceeded,newVersion) = ReplicationUtils.updateIsr(zkClient, topic, partitionId, localBrokerId,
              leaderEpoch, controllerEpoch, zkVersion, newInSyncReplicas)
            if(updateSucceeded) {
              inSyncReplicas = newInSyncReplicas
              zkVersion = newVersion
              trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newInSyncReplicas.mkString(","), zkVersion))
            } else {
              info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
            }
            // we may need to increment high watermark since ISR could be down to 1
            maybeIncrementLeaderHW(leaderReplica)
            replicaManager.isrShrinkRate.mark()
          }
        case None => // do nothing if no longer leader
      }
    }
  }

  def getOutOfSyncReplicas(leaderReplica: Replica, keepInSyncTimeMs: Long, keepInSyncMessages: Long): Set[Replica] = {
    /**
     * there are two cases that need to be handled here -
     * 1. Stuck followers: If the leo of the replica hasn't been updated for keepInSyncTimeMs ms,
     *                     the follower is stuck and should be removed from the ISR
     * 2. Slow followers: If the leo of the slowest follower is behind the leo of the leader by keepInSyncMessages, the
     *                     follower is not catching up and should be removed from the ISR
     **/
    val leaderLogEndOffset = leaderReplica.logEndOffset
    val candidateReplicas = inSyncReplicas - leaderReplica
    // Case 1 above
    val stuckReplicas = candidateReplicas.filter(r => (time.milliseconds - r.logEndOffsetUpdateTimeMs) > keepInSyncTimeMs)
    if(stuckReplicas.size > 0)
      debug("Stuck replicas for partition [%s,%d] are %s".format(topic, partitionId, stuckReplicas.map(_.brokerId).mkString(",")))
    // Case 2 above
    val slowReplicas = candidateReplicas.filter(r => r.logEndOffset >= 0 && (leaderLogEndOffset - r.logEndOffset) > keepInSyncMessages)
    if(slowReplicas.size > 0)
      debug("Slow replicas for partition [%s,%d] are %s".format(topic, partitionId, slowReplicas.map(_.brokerId).mkString(",")))
    stuckReplicas ++ slowReplicas
  }

  def appendMessagesToLeader(messages: ByteBufferMessageSet) = {
    inLock(leaderIsrUpdateLock.readLock()) {
      val leaderReplicaOpt = leaderReplicaIfLocal()
      leaderReplicaOpt match {
        case Some(leaderReplica) =>
          val log = leaderReplica.log.get
          val info = log.append(messages, assignOffsets = true)
          // we may need to increment high watermark since ISR could be down to 1
          maybeIncrementLeaderHW(leaderReplica)
          info
        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
            .format(topic, partitionId, localBrokerId))
      }
    }
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Partition]))
      return false
    val other = that.asInstanceOf[Partition]
    if(topic.equals(other.topic) && partitionId == other.partitionId)
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*partitionId
  }

  override def toString(): String = {
    inLock(leaderIsrUpdateLock.readLock()) {
      val partitionString = new StringBuilder
      partitionString.append("Topic: " + topic)
      partitionString.append("; Partition: " + partitionId)
      partitionString.append("; Leader: " + leaderReplicaIdOpt)
      partitionString.append("; AssignedReplicas: " + assignedReplicaMap.keys.mkString(","))
      partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
      partitionString.toString()
    }
  }
}

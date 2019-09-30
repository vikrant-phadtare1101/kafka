/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.controller

import kafka.admin.AdminOperationException
import kafka.common.StateChangeFailedException
import kafka.utils.Logging
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

import scala.collection.{Seq, Set, immutable}

/**
 * A helper class which contains logic for driving partition reassignments.
 * This class is not thread-safe.
 *
 * @param controllerEpoch - A function to return the latest controller epoch
 */
class ReassignmentManager(controllerContext: ControllerContext,
                          topicDeletionManager: TopicDeletionManager,
                          replicaStateMachine: ReplicaStateMachine,
                          partitionStateMachine: PartitionStateMachine,
                          stateChangeLogger: StateChangeLogger,
                          zkClient: KafkaZkClient,
                          brokerRequestBatch: ControllerBrokerRequestBatch,
                          partitionReassignmentHandler: PartitionReassignmentHandler,
                          controllerEpoch: () => Int) extends Logging {
  /**
   * This callback is invoked:
   * 1. By the AlterPartitionReassignments API
   * 2. By the reassigned partitions listener which is triggered when the /admin/reassign/partitions znode is created
   * 3. When an ongoing reassignment finishes - this is detected by a change in the partition's ISR znode
   * 4. Whenever a new broker comes up which is part of an ongoing reassignment
   * 5. On controller startup/failover
   *
   *
   * Reassigning replicas for a partition goes through a few steps listed in the code.
   * RS = current assigned replica set
   * ORS = Original replica set for partition
   * TRS = Reassigned (target) replica set
   * AR = The replicas we are adding as part of this reassignment
   * RR = The replicas we are removing as part of this reassignment
   *
   * A reassignment may have up to three phases, each with its own steps:
   *
   *
   * Cleanup Phase: In the cases where this reassignment has to override an ongoing reassignment.
   *   . The ongoing reassignment is in phase A
   *   . ORS denotes the original replica set, prior to the ongoing reassignment
   *   . URS denotes the unnecessary replicas, ones which are currently part of the AR of the ongoing reassignment but will not be part of the new one
   *   . OVRS denotes the overlapping replica set - replicas which are part of the AR of the ongoing reassignment and will be part of the overriding reassignment
   *       (it is essentially (RS - ORS) - URS)
   *
   *   1 Set RS = ORS + OVRS, AR = OVRS, RS = [] in memory
   *   2 Send LeaderAndIsr request with RS = ORS + OVRS, AR = [], RS = [] to all brokers in ORS + OVRS
   *     (because the ongoing reassignment is in phase A, we know we wouldn't have a leader in URS
   *      unless a preferred leader election was triggered while the reassignment was happening)
   *   3 Replicas in URS -> Offline (force those replicas out of ISR)
   *   4 Replicas in URS -> NonExistentReplica (force those replicas to be deleted)
   *
   * Phase A: Initial trigger (when TRS != ISR)
   *   A1. Update ZK with RS = ORS + TRS,
   *                      AR = TRS - ORS and
   *                      RR = ORS - TRS.
   *   A2. Update memory with RS = ORS + TRS, AR = TRS - ORS and RR = ORS - TRS
   *   A3. Send LeaderAndIsr request to every replica in ORS + TRS (with the new RS, AR and RR).
   *       We do this by forcing an update of the leader epoch in zookeeper.
   *   A4. Start new replicas AR by moving replicas in AR to NewReplica state.
   *
   * Phase B: All of TRS have caught up with the leaders and are in ISR
   *   B1. Move all replicas in TRS to OnlineReplica state.
   *   B2. Set RS = TRS, AR = [], RR = [] in memory.
   *   B3. Send a LeaderAndIsr request with RS = TRS. This will prevent the leader from adding any replica in TRS - ORS back in the isr.
   *       If the current leader is not in TRS or isn't alive, we move the leader to a new replica in TRS.
   *       We may send the LeaderAndIsr to more than the TRS replicas due to the
   *       way the partition state machine works (it reads replicas from ZK)
   *   B4. Move all replicas in RR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
   *       isr to remove RR in ZooKeeper and send a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
   *       After that, we send a StopReplica (delete = false) to the replicas in RR.
   *   B5. Move all replicas in RR to NonExistentReplica state. This will send a StopReplica (delete = true) to
   *       the replicas in RR to physically delete the replicas on disk.
   *   B6. Update ZK with RS=TRS, AR=[], RR=[].
   *   B7. Remove the ISR reassign listener and maybe update the /admin/reassign_partitions path in ZK to remove this partition from it if present.
   *   B8. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
   *
   * In general, there are two goals we want to aim for:
   * 1. Every replica present in the replica set of a LeaderAndIsrRequest gets the request sent to it
   * 2. Replicas that are removed from a partition's assignment get StopReplica sent to them
   *
   * For example, if ORS = {1,2,3} and TRS = {4,5,6}, the values in the topic and leader/isr paths in ZK
   * may go through the following transitions.
   * RS                AR          RR          leader     isr
   * {1,2,3}           {}          {}          1          {1,2,3}           (initial state)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     1          {1,2,3}           (step A2)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     1          {1,2,3,4,5,6}     (phase B)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     4          {1,2,3,4,5,6}     (step B3)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     4          {4,5,6}           (step B4)
   * {4,5,6}           {}          {}          4          {4,5,6}           (step B6)
   *
   * Note that we have to update RS in ZK with TRS last since it's the only place where we store ORS persistently.
   * This way, if the controller crashes before that step, we can still recover.
   *
   * @throws IllegalStateException
   */
  def onPartitionReassignment(topicPartition: TopicPartition, reassignedPartitionContext: ReassignedPartitionsContext): Unit = {
    val originalAssignmentOpt = maybeRevertOngoingReassignment(topicPartition, reassignedPartitionContext)
    val oldReplicas = originalAssignmentOpt match {
      case Some(originalReplicas) => originalReplicas
      case None => controllerContext.partitionFullReplicaAssignment(topicPartition).previousAssignment.replicas
    }
    // RS = ORS + TRS, AR = TRS - ORS and RR = ORS - TRS
    val partitionAssignment = PartitionReplicaAssignment.fromOldAndNewReplicas(
      oldReplicas = oldReplicas,
      newReplicas = reassignedPartitionContext.newReplicas)
    assert(reassignedPartitionContext.newReplicas == partitionAssignment.targetReplicas,
      s"newReplicas ${reassignedPartitionContext.newReplicas} were not equal to the expected targetReplicas ${partitionAssignment.targetReplicas}")
    val targetReplicas = partitionAssignment.targetReplicas

    if (!areReplicasInIsr(topicPartition, targetReplicas)) {
      info(s"New replicas ${targetReplicas.mkString(",")} for partition $topicPartition being reassigned not yet " +
        "caught up with the leader")

      // A1. Update ZK with RS = ORS + TRS, AR = TRS - ORS and RR = ORS - TRS.
      updateReplicaAssignmentForPartition(topicPartition, partitionAssignment)
      // A2. Update memory with RS = ORS + TRS, AR = TRS - ORS and RR = ORS - TRS
      controllerContext.updatePartitionFullReplicaAssignment(topicPartition, partitionAssignment)
      // A3. Send LeaderAndIsr request to every replica in ORS + TRS (with the new RS, AR and RR).
      val updatedAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
      updateLeaderEpochAndSendRequest(topicPartition, updatedAssignment.replicas, updatedAssignment)
      // A4. replicas in AR -> NewReplica
      startNewReplicasForReassignedPartition(topicPartition, updatedAssignment.addingReplicas)
      info(s"Waiting for new replicas ${updatedAssignment.addingReplicas.mkString(",")} for partition $topicPartition being " +
        s"reassigned to catch up with the leader (target replicas ${updatedAssignment.targetReplicas})")
    } else {
      // B1. replicas in TRS -> OnlineReplica
      targetReplicas.foreach { replica =>
        replicaStateMachine.handleStateChanges(Seq(PartitionAndReplica(topicPartition, replica)), OnlineReplica)
      }
      // B2. Set RS = TRS, AR = [], RR = [] in memory.
      // B3. Send LeaderAndIsr request with a potential new leader (if current leader not in TRS) and
      //   a new RS (using TRS) and same isr to every broker in ORS + TRS or TRS
      moveReassignedPartitionLeaderIfRequired(topicPartition, reassignedPartitionContext, partitionAssignment)
      // B4. replicas in RR -> Offline (force those replicas out of isr)
      // B5. replicas in RR -> NonExistentReplica (force those replicas to be deleted)
      stopRemovedReplicasOfReassignedPartition(topicPartition, partitionAssignment.removingReplicas)
      // B6. Update ZK with RS = TRS, AR = [], RR = [].
      updateReplicaAssignmentForPartition(topicPartition, controllerContext.partitionFullReplicaAssignment(topicPartition))
      // B7. Remove the ISR reassign listener and maybe update the /admin/reassign_partitions path in ZK to remove this partition from it.
      removePartitionFromReassignedPartitions(topicPartition)
      // B8. After electing a leader in B3, the replicas and isr information changes, so resend the update metadata request to every broker
      brokerRequestBatch.newBatch()
      brokerRequestBatch.addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
      brokerRequestBatch.sendRequestsToBrokers(controllerEpoch())
      // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
      topicDeletionManager.resumeDeletionForTopics(Set(topicPartition.topic))
    }
  }

  /**
   * This is called in case we need to override/revert an ongoing reassignment.
   * Note that due to the way we compute the original replica set, we have no guarantee that a revert would put it in the same order.
   * @return An option of the original replicas prior to the ongoing reassignment. None if there is no ongoing reassignment
   */
  private def maybeRevertOngoingReassignment(topicPartition: TopicPartition, reassignedPartitionContext: ReassignedPartitionsContext): Option[Seq[Int]] = {
    reassignedPartitionContext.ongoingReassignmentOpt match {
      case Some(ongoingAssignment) =>
        val originalAssignment = ongoingAssignment.previousAssignment
        assert(ongoingAssignment.isBeingReassigned)
        assert(!originalAssignment.isBeingReassigned)

        val unnecessaryReplicas = ongoingAssignment.replicas.filterNot(originalAssignment.replicas.contains(_))
        val (overlappingReplicas, replicasToRemove) = unnecessaryReplicas.partition(reassignedPartitionContext.newReplicas.contains(_))
        // RS = ORS + OVRS, AR = OVRS, RR = []
        val intermediateReplicaAssignment = PartitionReplicaAssignment(originalAssignment.replicas ++ overlappingReplicas, overlappingReplicas, Seq())

        if (isDebugEnabled)
          debug(s"Reverting previous reassignment $originalAssignment (we were in the " +
            s"process of an ongoing reassignment with target replicas ${ongoingAssignment.targetReplicas.mkString(",")} (" +
            s"Overlapping replicas: ${overlappingReplicas.mkString(",")}, Replicas to remove: ${replicasToRemove.mkString(",")})")

        // Set RS = ORS + OVRS, AR = OVRS, RR = [] in memory.
        controllerContext.updatePartitionFullReplicaAssignment(topicPartition, intermediateReplicaAssignment)

        // Increment leader epoch and send LeaderAndIsr with new RS (using old replicas and overlapping replicas) and same ISR to every broker in ORS + OVRS
        // This will prevent the leader from adding any replica outside RS back in the ISR
        updateLeaderEpochAndSendRequest(topicPartition, intermediateReplicaAssignment.replicas, intermediateReplicaAssignment)

        // replicas in URS -> Offline (force those replicas out of isr)
        // replicas in URS -> NonExistentReplica (force those replicas to be deleted)
        stopRemovedReplicasOfReassignedPartition(topicPartition, replicasToRemove)
        reassignedPartitionContext.ongoingReassignmentOpt = None
        Some(originalAssignment.replicas)
      case None => None // nothing to revert
    }
  }


  /**
   * @throws IllegalStateException
   */
  private def updateLeaderEpochAndSendRequest(partition: TopicPartition, replicasToReceiveRequest: Seq[Int],
                                              newAssignedReplicas: PartitionReplicaAssignment): Unit = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerEpoch())
    val replicaSetStr = s"replica set ${newAssignedReplicas.replicas.mkString(",")} " +
      s"(addingReplicas: ${newAssignedReplicas.addingReplicas.mkString(",")}, removingReplicas: ${newAssignedReplicas.removingReplicas.mkString(",")})"
    updateLeaderEpoch(partition) match {
      case Some(updatedLeaderIsrAndControllerEpoch) =>
        brokerRequestBatch.newBatch()
        brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasToReceiveRequest, partition,
          updatedLeaderIsrAndControllerEpoch, newAssignedReplicas, isNew = false)
        brokerRequestBatch.sendRequestsToBrokers(controllerEpoch())
        stateChangeLog.trace(s"Sent LeaderAndIsr request $updatedLeaderIsrAndControllerEpoch with new assigned $replicaSetStr" +
          s"to leader ${updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader} " +
          s"for partition being reassigned $partition")
      case None => // fail the reassignment
        stateChangeLog.error(s"Failed to send LeaderAndIsr request with new assigned $replicaSetStr " +
          s"to leader for partition being reassigned $partition")
    }
  }


  /**
   * Does not change leader or isr, but just increments the leader epoch
   *
   * @param partition partition
   * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
   */
  private def updateLeaderEpoch(partition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    debug(s"Updating leader epoch for partition $partition")
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      zkWriteCompleteOrUnnecessary = zkClient.getTopicPartitionStates(Seq(partition)).get(partition) match {
        case Some(leaderIsrAndControllerEpoch) =>
          val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
          val persistedControllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
          if (persistedControllerEpoch > controllerEpoch())
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably " +
              s"means the current controller with epoch ${controllerEpoch()} went through a soft failure and another " +
              s"controller was elected with epoch $persistedControllerEpoch. Aborting state change by this controller")
          // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
          // assigned replica list
          val newLeaderAndIsr = leaderAndIsr.newEpochAndZkVersion
          // update the new leadership decision in zookeeper or retry
          val UpdateLeaderAndIsrResult(finishedUpdates, _) =
            zkClient.updateLeaderAndIsr(immutable.Map(partition -> newLeaderAndIsr), controllerEpoch(), controllerContext.epochZkVersion)

          finishedUpdates.headOption.map {
            case (partition, Right(leaderAndIsr)) =>
              finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch()))
              info(s"Updated leader epoch for partition $partition to ${leaderAndIsr.leaderEpoch}")
              true
            case (_, Left(e)) =>
              throw e
          }.getOrElse(false)
        case None =>
          throw new IllegalStateException(s"Cannot update leader epoch for partition $partition as " +
            "leaderAndIsr path is empty. This could mean we somehow tried to reassign a partition that doesn't exist")
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  private def areReplicasInIsr(partition: TopicPartition, replicas: Seq[Int]): Boolean = {
    zkClient.getTopicPartitionStates(Seq(partition)).get(partition).exists { leaderIsrAndControllerEpoch =>
      replicas.forall(leaderIsrAndControllerEpoch.leaderAndIsr.isr.contains)
    }
  }

  private def moveReassignedPartitionLeaderIfRequired(topicPartition: TopicPartition,
                                                      reassignedPartitionContext: ReassignedPartitionsContext,
                                                      currentAssignment: PartitionReplicaAssignment): Unit = {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val currentLeader = controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader

    // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr
    // request to the current or new leader. This will prevent it from adding the old replicas to the ISR
    val newAssignment = PartitionReplicaAssignment(replicas = reassignedReplicas, addingReplicas = Seq(), removingReplicas = Seq())
    controllerContext.updatePartitionFullReplicaAssignment(
      topicPartition,
      newAssignment
    )

    if (!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is not in the new list of replicas ${reassignedReplicas.mkString(",")}. Re-electing leader")
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Some(ReassignPartitionLeaderElectionStrategy))
    } else {
      // check if the leader is alive or not
      if (controllerContext.isReplicaOnline(currentLeader, topicPartition)) {
        info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
          s"is already in the new list of replicas ${reassignedReplicas.mkString(",")} and is alive")
        // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
        updateLeaderEpochAndSendRequest(topicPartition, newAssignment.replicas, newAssignment)
      } else {
        info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
          s"is already in the new list of replicas ${reassignedReplicas.mkString(",")} but is dead")
        partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Some(ReassignPartitionLeaderElectionStrategy))
      }
    }
  }

  private def stopRemovedReplicasOfReassignedPartition(topicPartition: TopicPartition,
                                                       removedReplicas: Seq[Int]): Unit = {
    // first move the replica to offline state (the controller removes it from the ISR)
    val replicasToBeDeleted = removedReplicas.map(PartitionAndReplica(topicPartition, _))
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, OfflineReplica)
    // send stop replica command to the old replicas
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionStarted)
    // TODO: Eventually partition reassignment could use a callback that does retries if deletion failed
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionSuccessful)
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, NonExistentReplica)
  }

  private def updateReplicaAssignmentForPartition(partition: TopicPartition,
                                                  assignment: PartitionReplicaAssignment): Unit = {
    var topicAssignment = controllerContext.partitionFullReplicaAssignmentForTopic(partition.topic)
    topicAssignment += partition -> assignment

    val setDataResponse = zkClient.setTopicAssignmentRaw(partition.topic, topicAssignment, controllerContext.epochZkVersion)
    setDataResponse.resultCode match {
      case Code.OK =>
        info(s"Updated assigned replicas for partition $partition being reassigned to ${assignment.targetReplicas.mkString(",")}" +
          s" (addingReplicas: ${assignment.addingReplicas.mkString(",")}, removingReplicas: ${assignment.removingReplicas.mkString(",")})")
      case Code.NONODE => throw new IllegalStateException(s"Topic ${partition.topic} doesn't exist")
      case _ => throw new KafkaException(setDataResponse.resultException.get)
    }
  }

  private def startNewReplicasForReassignedPartition(topicPartition: TopicPartition, newReplicas: Seq[Int]): Unit = {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Seq(PartitionAndReplica(topicPartition, replica)), NewReplica)
    }
  }

  private def removePartitionFromReassignedPartitions(partitionToBeRemoved: TopicPartition) {
    controllerContext.partitionsBeingReassigned.get(partitionToBeRemoved) match {
      case Some(reassignContext) =>
        reassignContext.unregisterReassignIsrChangeHandler(zkClient)

        if (reassignContext.persistedInZk) {
          removePartitionsFromZkReassignment(Set(partitionToBeRemoved))
        }

        controllerContext.partitionsBeingReassigned.remove(partitionToBeRemoved)
      case None =>
        throw new IllegalStateException("Cannot remove a reassigning partition because it is not present in memory")
    }
  }

  def removePartitionsFromZkReassignment(partitionsToBeRemoved: Set[TopicPartition]): Unit = {
    if (!zkClient.reassignPartitionsInProgress()) {
      debug(s"Cannot remove partitions $partitionsToBeRemoved from ZooKeeper because there is no ZooKeeper reassignment present")
      return
    }

    val updatedPartitionsBeingReassigned = controllerContext.partitionsBeingReassigned.filter(_._2.persistedInZk).toMap -- partitionsToBeRemoved

    info(s"Removing partitions $partitionsToBeRemoved from the list of reassigned partitions in zookeeper")

    // write the new list to zookeeper
    if (updatedPartitionsBeingReassigned.isEmpty) {
      zkClient.deletePartitionReassignment(controllerContext.epochZkVersion)
      // The znode was deleted. We register a ZK watcher to ensure we detect future reassignments
      zkClient.registerZNodeChangeHandlerAndCheckExistence(partitionReassignmentHandler)
    } else {
      val reassignment = updatedPartitionsBeingReassigned.map { case (k, v) => k -> v.newReplicas }
      try zkClient.setOrCreatePartitionReassignment(reassignment, controllerContext.epochZkVersion)
      catch {
        case e: KeeperException => throw new AdminOperationException(e)
      }
    }
  }
}

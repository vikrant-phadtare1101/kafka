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

import kafka.api.LeaderAndIsr
import kafka.cluster.Broker
import kafka.zk.KafkaZkClient
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zookeeper.SetDataResponse
import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.KeeperException.Code
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}
import org.mockito.Mockito
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.verify

import scala.collection.{Map, Set, mutable}

class ReassignmentManagerTest {
  private var controllerContext: ControllerContext = null
  private var mockZkClient: KafkaZkClient = null
  private var mockTopicDeletionmManager: TopicDeletionManager = null
  private var mockControllerBrokerRequestBatch: ControllerBrokerRequestBatch = null
  private var mockReplicaStateMachine: ReplicaStateMachine = null
  private var mockPartitionStateMachine: PartitionStateMachine = null
  private var mockBrokerRequestBatch: ControllerBrokerRequestBatch = null

  private final val controllerEpoch = 10
  private final val zkEpoch = 105
  private final val topic = "topic"
  private final val tp = new TopicPartition(topic, 0)
  private final val mockPartitionReassignmentHandler = new PartitionReassignmentHandler(null)

  private var partitionReassignmentManager: ReassignmentManager = null

  @Before
  def setUp(): Unit = {
    controllerContext = new ControllerContext
    controllerContext.epoch = controllerEpoch
    controllerContext.epochZkVersion = zkEpoch
    mockZkClient = Mockito.mock(classOf[KafkaZkClient])
    mockTopicDeletionmManager = Mockito.mock(classOf[TopicDeletionManager])
    mockControllerBrokerRequestBatch = Mockito.mock(classOf[ControllerBrokerRequestBatch])
    mockReplicaStateMachine = Mockito.mock(classOf[ReplicaStateMachine])
    mockPartitionStateMachine = Mockito.mock(classOf[PartitionStateMachine])
    mockBrokerRequestBatch = Mockito.mock(classOf[ControllerBrokerRequestBatch])

    partitionReassignmentManager = new ReassignmentManager(controllerContext, mockTopicDeletionmManager,
      mockReplicaStateMachine, mockPartitionStateMachine, new StateChangeLogger(0, inControllerContext = true, None),
      mockZkClient, mockBrokerRequestBatch, mockPartitionReassignmentHandler,
      controllerEpoch = () => controllerEpoch)
  }

  /**
   * Phase A of a partition reassignment denotes the initial trigger of a reassignment.
   *
   *  A1. Update ZK with RS = ORS + TRS,
   *                     AR = TRS - ORS and
   *                     RR = ORS - TRS.
   * A2. Update memory with RS = ORS + TRS, AR = TRS - ORS and RR = ORS - TRS
   * A3. Send LeaderAndIsr request to every replica in ORS + TRS (with the new RS, AR and RR).
   *     We do this by forcing an update of the leader epoch in zookeeper.
   * A4. Start new replicas AR by moving replicas in AR to NewReplica state.
   */
  @Test
  def testPhaseAOfPartitionReassignment(): Unit = {
    /*
     * Existing assignment is [1,2,3]
     * We issue a reassignment to [3, 4, 5]
     */
    val reassignContext = ReassignedPartitionsContext(Seq(3, 4, 5), null, persistedInZk = false,
      ongoingReassignmentOpt = None)
    val expectedAddingReplicas = Seq(4, 5)
    val expectedFullReplicaSet = Seq(3, 4, 5, 1, 2)
    val initialAssignment = PartitionReplicaAssignment(Seq(1, 2, 3), Seq(), Seq())
    val initialLeaderAndIsr = new LeaderAndIsr(1, 1, List(1, 2, 3), zkEpoch)
    controllerContext.partitionAssignments.put(topic, mutable.Map(tp.partition() -> initialAssignment))
    controllerContext.updatePartitionFullReplicaAssignment(tp, initialAssignment)
    controllerContext.partitionsBeingReassigned.put(tp, reassignContext)

    mockAreReplicasInIsr(tp, List(1, 2, 3), initialLeaderAndIsr)
    val expectedNewAssignment = PartitionReplicaAssignment.fromOldAndNewReplicas(Seq(1, 2, 3), Seq(3, 4, 5))
    assertEquals(expectedFullReplicaSet, expectedNewAssignment.replicas)
    // A1. Should update ZK
    doReturn(mockSetDataResponseOK, Nil: _*).when(mockZkClient).setTopicAssignmentRaw(topic, mutable.Map(tp -> expectedNewAssignment), zkEpoch)
    // A2. Should update memory
    // A3. Should update partition leader epoch in ZK
    val expectedLeaderAndIsr = initialLeaderAndIsr.newEpochAndZkVersion
    doReturn(UpdateLeaderAndIsrResult(Map(tp -> Right(expectedLeaderAndIsr)), Seq()), Nil: _*)
      .when(mockZkClient).updateLeaderAndIsr(Map(tp -> expectedLeaderAndIsr), controllerEpoch, zkEpoch)

    // act
    partitionReassignmentManager.onPartitionReassignment(tp, reassignContext)

    // A2. Should have updated memory
    assertEquals(expectedNewAssignment, controllerContext.partitionFullReplicaAssignment(tp))
    // A3. Should send a LeaderAndIsr request to every replica in ORS + TRS (with the new RS, AR and RR).
    verify(mockBrokerRequestBatch).addLeaderAndIsrRequestForBrokers(
      expectedFullReplicaSet, tp,
      LeaderIsrAndControllerEpoch(expectedLeaderAndIsr, controllerEpoch), expectedNewAssignment, isNew = false
    )
    // A4. replicas in AR -> NewReplica
    expectedAddingReplicas.foreach { newReplica =>
      verify(mockReplicaStateMachine).handleStateChanges(
        Seq(PartitionAndReplica(tp, newReplica)), NewReplica
      )
    }
    verify(mockBrokerRequestBatch).sendRequestsToBrokers(controllerEpoch)
  }

  /**
   * Phase B of a partition reassignment is the part where all the new replicas are in ISR
   *  and the controller finishes the reassignment
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
   */
  @Test
  def testPhaseBOfPartitionReassignment(): Unit = {
    /*
     * Existing assignment is [1,2,3]
     * We had issued a reassignment to [3, 4, 5] and now all replicas are in ISR
     */
    val mockIsrChangeHandler = new PartitionReassignmentIsrChangeHandler(null, tp)
    val reassignContext = ReassignedPartitionsContext(Seq(3, 4, 5), mockIsrChangeHandler, persistedInZk = true,
      ongoingReassignmentOpt = None)
    val expectedRemovingReplicas = Seq(PartitionAndReplica(tp, 1), PartitionAndReplica(tp, 2))
    val targetReplicas = Seq(3, 4, 5)
    val brokerIdsAndEpochs = Map(Broker(1, Seq(), None) -> 1L,
      Broker(2, Seq(), None) -> 1L, Broker(3, Seq(), None) -> 1L,
      Broker(4, Seq(), None) -> 1L, Broker(5, Seq(), None) -> 1L,
      Broker(6, Seq(), None) -> 1L)
    val initialAssignment = PartitionReplicaAssignment.fromOldAndNewReplicas(Seq(1, 2, 3), Seq(3, 4, 5))
    val expectedNewAssignment = PartitionReplicaAssignment(Seq(3, 4, 5), Seq(), Seq())
    val initialLeaderAndIsr = new LeaderAndIsr(1, 1, List(1, 2, 3, 4, 5), zkEpoch)
    controllerContext.addLiveBrokersAndEpochs(brokerIdsAndEpochs)
    controllerContext.partitionAssignments.put(topic, mutable.Map(tp.partition() -> initialAssignment))
    controllerContext.updatePartitionFullReplicaAssignment(tp, initialAssignment)
    controllerContext.partitionsBeingReassigned.put(tp, reassignContext)
    controllerContext.partitionLeadershipInfo.put(tp,
      LeaderIsrAndControllerEpoch(initialLeaderAndIsr, controllerEpoch))
    mockAreReplicasInIsr(tp, List(1, 2, 3, 4, 5), initialLeaderAndIsr)

    // B2. Set RS = TRS, AR = [], RR = [] in memory.
    // B3. Send LeaderAndIsr request with a potential new leader (if current leader not in TRS) and
    //   a new RS (using TRS) and same isr to every broker in ORS + TRS or TRS
    doReturn(Map.empty[TopicPartition, Either[Throwable, LeaderAndIsr]], Nil: _*)
      .when(mockPartitionStateMachine).handleStateChanges(Seq(tp), OnlinePartition, Some(ReassignPartitionLeaderElectionStrategy))
    // B6. Update ZK with RS = TRS, AR = [], RR = [].
    doReturn(mockSetDataResponseOK, Nil: _*).when(mockZkClient)
      .setTopicAssignmentRaw(tp.topic(), mutable.Map(tp -> expectedNewAssignment), zkEpoch)
    // B7. Should remove partition from in-memory partitionsBeingReassigned,
    // remove partition from admin/partition_reassignments znode, delete the znode (since it's the last partition)
    // and register a new watcher
    doReturn(true, Nil: _*).when(mockZkClient).reassignPartitionsInProgress()
    doReturn(false, Nil: _*).when(mockZkClient).registerZNodeChangeHandlerAndCheckExistence(mockPartitionReassignmentHandler)

    // act
    partitionReassignmentManager.onPartitionReassignment(tp, reassignContext)

    // B1. All target replicas moved to OnlineReplica state.
    targetReplicas.foreach { targetReplica =>
      verify(mockReplicaStateMachine).handleStateChanges(
        Seq(PartitionAndReplica(tp, targetReplica)), OnlineReplica
      )
    }
    // B2. Should have updated memory
    assertEquals(expectedNewAssignment, controllerContext.partitionFullReplicaAssignment(tp))
    // B4. replicas in RR -> Offline (force those replicas out of isr)
    // B5. replicas in RR -> NonExistentReplica (force those replicas to be deleted)
    verify(mockReplicaStateMachine).handleStateChanges(expectedRemovingReplicas, OfflineReplica)
    verify(mockReplicaStateMachine).handleStateChanges(expectedRemovingReplicas, ReplicaDeletionStarted)
    verify(mockReplicaStateMachine).handleStateChanges(expectedRemovingReplicas, ReplicaDeletionSuccessful)
    verify(mockReplicaStateMachine).handleStateChanges(expectedRemovingReplicas, NonExistentReplica)
    // B7. Should have cleared in-memory partitionsBeingReassigned
    assertEquals(mutable.Map(), controllerContext.partitionsBeingReassigned)
    // B8. Resend the update metadata request to every broker
    verify(mockBrokerRequestBatch).addUpdateMetadataRequestForBrokers(brokerIdsAndEpochs.keys.map(_.id).toSeq, Set(tp))
    verify(mockBrokerRequestBatch).sendRequestsToBrokers(controllerEpoch)
    verify(mockTopicDeletionmManager).resumeDeletionForTopics(Set(tp.topic()))
  }

  /**
   * To determine what phase of the reassignment we are in, we check whether the target replicas are in the ISR set
   * If they aren't, we enter phase A. If they are - phase B
   */
  def mockAreReplicasInIsr(tp: TopicPartition, isr: List[Int], leaderAndIsr: LeaderAndIsr): Unit = {
    val tpStateMap: Map[TopicPartition, LeaderIsrAndControllerEpoch] = Map(
      tp -> LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    )
    doReturn(tpStateMap, Nil: _*).when(mockZkClient).getTopicPartitionStates(Seq(tp))
  }

  def mockSetDataResponseOK: SetDataResponse =
    SetDataResponse(Code.OK, "", None, null, null)
}

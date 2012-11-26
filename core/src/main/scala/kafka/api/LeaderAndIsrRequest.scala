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


package kafka.api

import java.nio._
import kafka.utils._
import kafka.api.ApiUtils._
import kafka.cluster.Broker
import kafka.controller.LeaderIsrAndControllerEpoch


object LeaderAndIsr {
  val initialLeaderEpoch: Int = 0
  val initialZKVersion: Int = 0
}

case class LeaderAndIsr(var leader: Int, var leaderEpoch: Int, var isr: List[Int], var zkVersion: Int) {
  def this(leader: Int, isr: List[Int]) = this(leader, LeaderAndIsr.initialLeaderEpoch, isr, LeaderAndIsr.initialZKVersion)

  override def toString(): String = {
    val jsonDataMap = new collection.mutable.HashMap[String, String]
    jsonDataMap.put("leader", leader.toString)
    jsonDataMap.put("leaderEpoch", leaderEpoch.toString)
    jsonDataMap.put("ISR", isr.mkString(","))
    Utils.stringMapToJson(jsonDataMap)
  }
}

object PartitionStateInfo {
  def readFrom(buffer: ByteBuffer): PartitionStateInfo = {
    val controllerEpoch = buffer.getInt
    val leader = buffer.getInt
    val leaderEpoch = buffer.getInt
    val isrString = readShortString(buffer)
    val isr = isrString.split(",").map(_.toInt).toList
    val zkVersion = buffer.getInt
    val replicationFactor = buffer.getInt
    PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, leaderEpoch, isr, zkVersion), controllerEpoch),
      replicationFactor)
  }
}

case class PartitionStateInfo(val leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch, val replicationFactor: Int) {
  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(leaderIsrAndControllerEpoch.controllerEpoch)
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.leader)
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch)
    writeShortString(buffer, leaderIsrAndControllerEpoch.leaderAndIsr.isr.mkString(","))
    buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion)
    buffer.putInt(replicationFactor)
  }

  def sizeInBytes(): Int = {
    val size =
      4 /* epoch of the controller that elected the leader */ +
      4 /* leader broker id */ +
      4 /* leader epoch */ +
      (2 + leaderIsrAndControllerEpoch.leaderAndIsr.isr.mkString(",").length) +
      4 /* zk version */ +
      4 /* replication factor */
    size
  }
}

object LeaderAndIsrRequest {
  val CurrentVersion = 1.shortValue()
  val DefaultClientId = ""
  val IsInit: Boolean = true
  val NotInit: Boolean = false
  val DefaultAckTimeout: Int = 1000

  def readFrom(buffer: ByteBuffer): LeaderAndIsrRequest = {
    val versionId = buffer.getShort
    val clientId = readShortString(buffer)
    val ackTimeoutMs = buffer.getInt
    val controllerEpoch = buffer.getInt
    val partitionStateInfosCount = buffer.getInt
    val partitionStateInfos = new collection.mutable.HashMap[(String, Int), PartitionStateInfo]

    for(i <- 0 until partitionStateInfosCount){
      val topic = readShortString(buffer)
      val partition = buffer.getInt
      val partitionStateInfo = PartitionStateInfo.readFrom(buffer)

      partitionStateInfos.put((topic, partition), partitionStateInfo)
    }

    val leadersCount = buffer.getInt
    var leaders = Set[Broker]()
    for (i <- 0 until leadersCount)
      leaders += Broker.readFrom(buffer)

    new LeaderAndIsrRequest(versionId, clientId, ackTimeoutMs, partitionStateInfos.toMap, leaders, controllerEpoch)
  }
}

case class LeaderAndIsrRequest (versionId: Short,
                                clientId: String,
                                ackTimeoutMs: Int,
                                partitionStateInfos: Map[(String, Int), PartitionStateInfo],
                                leaders: Set[Broker],
                                controllerEpoch: Int)
        extends RequestOrResponse(Some(RequestKeys.LeaderAndIsrKey)) {

  def this(partitionStateInfos: Map[(String, Int), PartitionStateInfo], liveBrokers: Set[Broker], controllerEpoch: Int) = {
    this(LeaderAndIsrRequest.CurrentVersion, LeaderAndIsrRequest.DefaultClientId, LeaderAndIsrRequest.DefaultAckTimeout,
      partitionStateInfos, liveBrokers, controllerEpoch)
  }

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    writeShortString(buffer, clientId)
    buffer.putInt(ackTimeoutMs)
    buffer.putInt(controllerEpoch)
    buffer.putInt(partitionStateInfos.size)
    for((key, value) <- partitionStateInfos){
      writeShortString(buffer, key._1)
      buffer.putInt(key._2)
      value.writeTo(buffer)
    }
    buffer.putInt(leaders.size)
    leaders.foreach(_.writeTo(buffer))
  }

  def sizeInBytes(): Int = {
    var size =
      2 /* version id */ +
      (2 + clientId.length) /* client id */ +
      4 /* ack timeout */ +
      4 /* controller epoch */ +
      4 /* number of partitions */
    for((key, value) <- partitionStateInfos)
      size += (2 + key._1.length) /* topic */ + 4 /* partition */ + value.sizeInBytes /* partition state info */
    size += 4 /* number of leader brokers */
    for(broker <- leaders)
      size += broker.sizeInBytes /* broker info */
    size
  }
}
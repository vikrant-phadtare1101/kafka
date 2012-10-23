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

import org.scalatest.junit.JUnit3Suite
import org.junit.Assert._
import kafka.admin.CreateTopicCommand
import kafka.utils.TestUtils._
import kafka.utils.{Utils, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import kafka.message.Message
import kafka.producer.{ProducerConfig, ProducerData, Producer}

class LogRecoveryTest extends JUnit3Suite with ZooKeeperTestHarness {

  val configs = TestUtils.createBrokerConfigs(2).map(new KafkaConfig(_) {
    override val replicaMaxLagTimeMs = 5000L
    override val replicaMaxLagBytes = 10L
    override val flushInterval = 10
    override val replicaMinBytes = 20
  })
  val topic = "new-topic"
  val partitionId = 0

  var server1: KafkaServer = null
  var server2: KafkaServer = null

  val configProps1 = configs.head
  val configProps2 = configs.last

  val message = new Message("hello".getBytes())

  var producer: Producer[Int, Message] = null
  var hwFile1: HighwaterMarkCheckpoint = new HighwaterMarkCheckpoint(configProps1.logDir)
  var hwFile2: HighwaterMarkCheckpoint = new HighwaterMarkCheckpoint(configProps2.logDir)
  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

  def testHWCheckpointNoFailuresSingleLogSegment {
    // start both servers
    server1 = TestUtils.createServer(configProps1)
    server2 = TestUtils.createServer(configProps2)
    servers ++= List(server1, server2)

    val producerProps = getProducerConfig(TestUtils.getBrokerListStrFromConfigs(configs), 64*1024, 100000, 10000)
    producerProps.put("producer.request.timeout.ms", "1000")
    producerProps.put("producer.request.required.acks", "-1")
    producer = new Producer[Int, Message](new ProducerConfig(producerProps))

    // create topic with 1 partition, 2 replicas, one on each broker
    CreateTopicCommand.createTopic(zkClient, topic, 1, 2, configs.map(_.brokerId).mkString(":"))

    // wait until leader is elected
    var leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 500)
    assertTrue("Leader should get elected", leader.isDefined)
    // NOTE: this is to avoid transient test failures
    assertTrue("Leader could be broker 0 or broker 1", (leader.getOrElse(-1) == 0) || (leader.getOrElse(-1) == 1))

    val numMessages = 2L
    sendMessages(numMessages.toInt)

    // give some time for the follower 1 to record leader HW
    assertTrue("Failed to update highwatermark for follower after 1000 ms", 
               TestUtils.waitUntilTrue(() =>
                 server2.replicaManager.getReplica(topic, 0).get.highWatermark == numMessages, 10000))

    servers.foreach(server => server.replicaManager.checkpointHighWatermarks())
    producer.close()
    val leaderHW = hwFile1.read(topic, 0)
    assertEquals(numMessages, leaderHW)
    val followerHW = hwFile2.read(topic, 0)
    assertEquals(numMessages, followerHW)
    servers.foreach(server => { server.shutdown(); Utils.rm(server.config.logDir)})
  }

  def testHWCheckpointWithFailuresSingleLogSegment {
    // start both servers
    server1 = TestUtils.createServer(configProps1)
    server2 = TestUtils.createServer(configProps2)
    servers ++= List(server1, server2)

    val producerProps = getProducerConfig(TestUtils.getBrokerListStrFromConfigs(configs), 64*1024, 100000, 10000)
    producerProps.put("producer.request.timeout.ms", "1000")
    producerProps.put("producer.request.required.acks", "-1")
    producer = new Producer[Int, Message](new ProducerConfig(producerProps))

    // create topic with 1 partition, 2 replicas, one on each broker
    CreateTopicCommand.createTopic(zkClient, topic, 1, 2, configs.map(_.brokerId).mkString(":"))

    // wait until leader is elected
    var leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 500)
    assertTrue("Leader should get elected", leader.isDefined)
    // NOTE: this is to avoid transient test failures
    assertTrue("Leader could be broker 0 or broker 1", (leader.getOrElse(-1) == 0) || (leader.getOrElse(-1) == 1))
    
    assertEquals(0L, hwFile1.read(topic, 0))

    sendMessages(1)
    Thread.sleep(1000)
    var hw = 1L

    // kill the server hosting the preferred replica
    server1.shutdown()
    assertEquals(hw, hwFile1.read(topic, 0))

    // check if leader moves to the other server
    leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 500, leader)
    assertEquals("Leader must move to broker 1", 1, leader.getOrElse(-1))

    // bring the preferred replica back
    server1.startup()

    leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 500)
    assertTrue("Leader must remain on broker 1, in case of zookeeper session expiration it can move to broker 0",
      leader.isDefined && (leader.get == 0 || leader.get == 1))

    assertEquals(hw, hwFile1.read(topic, 0))
    // since server 2 was never shut down, the hw value of 30 is probably not checkpointed to disk yet
    server2.shutdown()
    assertEquals(hw, hwFile2.read(topic, 0))

    server2.startup()
    leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 500, leader)
    assertTrue("Leader must remain on broker 0, in case of zookeeper session expiration it can move to broker 1",
      leader.isDefined && (leader.get == 0 || leader.get == 1))

    sendMessages(1)
    hw += 1
      
    // give some time for follower 1 to record leader HW of 60
    assertTrue("Failed to update highwatermark for follower after 1000 ms", TestUtils.waitUntilTrue(() =>
      server2.replicaManager.getReplica(topic, 0).get.highWatermark == hw, 2000))
    // shutdown the servers to allow the hw to be checkpointed
    servers.foreach(server => server.shutdown())
    producer.close()
    assertEquals(hw, hwFile1.read(topic, 0))
    assertEquals(hw, hwFile2.read(topic, 0))
    servers.foreach(server => Utils.rm(server.config.logDir))
  }

  def testHWCheckpointNoFailuresMultipleLogSegments {
    val configs = TestUtils.createBrokerConfigs(2).map(new KafkaConfig(_) {
      override val replicaMaxLagTimeMs = 5000L
      override val replicaMaxLagBytes = 10L
      override val flushInterval = 10
      override val replicaMinBytes = 20
      override val logFileSize = 30
    })

    // start both servers
    server1 = TestUtils.createServer(configs.head)
    server2 = TestUtils.createServer(configs.last)
    servers ++= List(server1, server2)

    hwFile1 = new HighwaterMarkCheckpoint(server1.config.logDir)
    hwFile2 = new HighwaterMarkCheckpoint(server2.config.logDir)

    val producerProps = getProducerConfig(TestUtils.getBrokerListStrFromConfigs(configs), 64*1024, 100000, 10000)
    producerProps.put("producer.request.timeout.ms", "1000")
    producerProps.put("producer.request.required.acks", "-1")
    producer = new Producer[Int, Message](new ProducerConfig(producerProps))

    // create topic with 1 partition, 2 replicas, one on each broker
    CreateTopicCommand.createTopic(zkClient, topic, 1, 2, configs.map(_.brokerId).mkString(":"))

    // wait until leader is elected
    var leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 500)
    assertTrue("Leader should get elected", leader.isDefined)
    // NOTE: this is to avoid transient test failures
    assertTrue("Leader could be broker 0 or broker 1", (leader.getOrElse(-1) == 0) || (leader.getOrElse(-1) == 1))
    sendMessages(20)
    var hw = 20L
    // give some time for follower 1 to record leader HW of 600
    assertTrue("Failed to update highwatermark for follower after 1000 ms", TestUtils.waitUntilTrue(() =>
      server2.replicaManager.getReplica(topic, 0).get.highWatermark == hw, 1000))
    // shutdown the servers to allow the hw to be checkpointed
    servers.foreach(server => server.shutdown())
    producer.close()
    val leaderHW = hwFile1.read(topic, 0)
    assertEquals(hw, leaderHW)
    val followerHW = hwFile2.read(topic, 0)
    assertEquals(hw, followerHW)
    servers.foreach(server => Utils.rm(server.config.logDir))
  }

  def testHWCheckpointWithFailuresMultipleLogSegments {
    val configs = TestUtils.createBrokerConfigs(2).map(new KafkaConfig(_) {
      override val replicaMaxLagTimeMs = 5000L
      override val replicaMaxLagBytes = 10L
      override val flushInterval = 1000
      override val replicaMinBytes = 20
      override val logFileSize = 30
    })

    // start both servers
    server1 = TestUtils.createServer(configs.head)
    server2 = TestUtils.createServer(configs.last)
    servers ++= List(server1, server2)

    hwFile1 = new HighwaterMarkCheckpoint(server1.config.logDir)
    hwFile2 = new HighwaterMarkCheckpoint(server2.config.logDir)

    val producerProps = getProducerConfig(TestUtils.getBrokerListStrFromConfigs(configs), 64*1024, 100000, 10000)
    producerProps.put("producer.request.timeout.ms", "1000")
    producerProps.put("producer.request.required.acks", "-1")
    producer = new Producer[Int, Message](new ProducerConfig(producerProps))

    // create topic with 1 partition, 2 replicas, one on each broker
    CreateTopicCommand.createTopic(zkClient, topic, 1, 2, configs.map(_.brokerId).mkString(":"))

    // wait until leader is elected
    var leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 500)
    assertTrue("Leader should get elected", leader.isDefined)
    // NOTE: this is to avoid transient test failures
    assertTrue("Leader could be broker 0 or broker 1", (leader.getOrElse(-1) == 0) || (leader.getOrElse(-1) == 1))

    sendMessages(2)
    var hw = 2L
    
    // allow some time for the follower to get the leader HW
    assertTrue("Failed to update highwatermark for follower after 1000 ms", TestUtils.waitUntilTrue(() =>
      server2.replicaManager.getReplica(topic, 0).get.highWatermark == hw, 1000))
    // kill the server hosting the preferred replica
    server1.shutdown()
    server2.shutdown()
    assertEquals(hw, hwFile1.read(topic, 0))
    assertEquals(hw, hwFile2.read(topic, 0))

    server2.startup()
    // check if leader moves to the other server
    leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 500, leader)
    assertEquals("Leader must move to broker 1", 1, leader.getOrElse(-1))

    assertEquals(hw, hwFile1.read(topic, 0))

    // bring the preferred replica back
    server1.startup()

    assertEquals(hw, hwFile1.read(topic, 0))
    assertEquals(hw, hwFile2.read(topic, 0))

    sendMessages(2)
    hw += 2
    
    // allow some time for the follower to get the leader HW
    assertTrue("Failed to update highwatermark for follower after 1000 ms", TestUtils.waitUntilTrue(() =>
      server1.replicaManager.getReplica(topic, 0).get.highWatermark == hw, 1000))
    // shutdown the servers to allow the hw to be checkpointed
    servers.foreach(server => server.shutdown())
    producer.close()
    assertEquals(hw, hwFile1.read(topic, 0))
    assertEquals(hw, hwFile2.read(topic, 0))
    servers.foreach(server => Utils.rm(server.config.logDir))
  }

  private def sendMessages(n: Int = 1) {
    for(i <- 0 until n)
      producer.send(new ProducerData[Int, Message](topic, 0, message))
  }
}

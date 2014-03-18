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

package kafka.integration

import scala.collection.mutable.MutableList
import scala.util.Random
import org.apache.log4j.{Level, Logger}
import org.scalatest.junit.JUnit3Suite
import java.util.Properties
import junit.framework.Assert._
import kafka.admin.AdminUtils
import kafka.common.FailedToSendMessageException
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerTimeoutException}
import kafka.producer.{KeyedMessage, Producer}
import kafka.serializer.{DefaultEncoder, StringEncoder}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.Utils
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness

class UncleanLeaderElectionTest extends JUnit3Suite with ZooKeeperTestHarness {
  val brokerId1 = 0
  val brokerId2 = 1

  val port1 = choosePort()
  val port2 = choosePort()

  // controlled shutdown is needed for these tests, but we can trim the retry count and backoff interval to
  // reduce test execution time
  val enableControlledShutdown = true
  val configProps1 = createBrokerConfig(brokerId1, port1)
  val configProps2 = createBrokerConfig(brokerId2, port2)

  for (configProps <- List(configProps1, configProps2)) {
    configProps.put("controlled.shutdown.enable", String.valueOf(enableControlledShutdown))
    configProps.put("controlled.shutdown.max.retries", String.valueOf(1))
    configProps.put("controlled.shutdown.retry.backoff.ms", String.valueOf(1000))
  }

  var configs: Seq[KafkaConfig] = Seq.empty[KafkaConfig]
  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

  val random = new Random()
  val topic = "topic" + random.nextLong
  val partitionId = 0

  val kafkaApisLogger = Logger.getLogger(classOf[kafka.server.KafkaApis])
  val networkProcessorLogger = Logger.getLogger(classOf[kafka.network.Processor])
  val syncProducerLogger = Logger.getLogger(classOf[kafka.producer.SyncProducer])
  val eventHandlerLogger = Logger.getLogger(classOf[kafka.producer.async.DefaultEventHandler[Object, Object]])

  override def setUp() {
    super.setUp()

    // temporarily set loggers to a higher level so that tests run quietly
    kafkaApisLogger.setLevel(Level.FATAL)
    networkProcessorLogger.setLevel(Level.FATAL)
    syncProducerLogger.setLevel(Level.FATAL)
    eventHandlerLogger.setLevel(Level.FATAL)
  }

  override def tearDown() {
    servers.map(server => shutdownServer(server))
    servers.map(server => Utils.rm(server.config.logDirs))

    // restore log levels
    kafkaApisLogger.setLevel(Level.ERROR)
    networkProcessorLogger.setLevel(Level.ERROR)
    syncProducerLogger.setLevel(Level.ERROR)
    eventHandlerLogger.setLevel(Level.ERROR)

    super.tearDown()
  }

  private def startBrokers(cluster: Seq[Properties]) {
    for (props <- cluster) {
      val config = new KafkaConfig(props)
      val server = createServer(config)
      configs ++= List(config)
      servers ++= List(server)
    }
  }

  def testUncleanLeaderElectionEnabled {
    // unclean leader election is enabled by default
    startBrokers(Seq(configProps1, configProps2))

    // create topic with 1 partition, 2 replicas, one on each broker
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, Map(partitionId -> Seq(brokerId1, brokerId2)))

    verifyUncleanLeaderElectionEnabled
  }

  def testUncleanLeaderElectionDisabled {
	// disable unclean leader election
	configProps1.put("unclean.leader.election.enable", String.valueOf(false))
	configProps2.put("unclean.leader.election.enable", String.valueOf(false))
    startBrokers(Seq(configProps1, configProps2))

    // create topic with 1 partition, 2 replicas, one on each broker
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, Map(partitionId -> Seq(brokerId1, brokerId2)))

    verifyUncleanLeaderElectionDisabled
  }

  def testUncleanLeaderElectionEnabledByTopicOverride {
    // disable unclean leader election globally, but enable for our specific test topic
    configProps1.put("unclean.leader.election.enable", String.valueOf(false))
    configProps2.put("unclean.leader.election.enable", String.valueOf(false))
    startBrokers(Seq(configProps1, configProps2))

    // create topic with 1 partition, 2 replicas, one on each broker, and unclean leader election enabled
    val topicProps = new Properties()
    topicProps.put("unclean.leader.election.enable", String.valueOf(true))
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, Map(partitionId -> Seq(brokerId1, brokerId2)),
      topicProps)

    verifyUncleanLeaderElectionEnabled
  }

  def testCleanLeaderElectionDisabledByTopicOverride {
    // enable unclean leader election globally, but disable for our specific test topic
    configProps1.put("unclean.leader.election.enable", String.valueOf(true))
    configProps2.put("unclean.leader.election.enable", String.valueOf(true))
    startBrokers(Seq(configProps1, configProps2))

    // create topic with 1 partition, 2 replicas, one on each broker, and unclean leader election disabled
    val topicProps = new Properties()
    topicProps.put("unclean.leader.election.enable", String.valueOf(false))
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, Map(partitionId -> Seq(brokerId1, brokerId2)),
      topicProps)

    verifyUncleanLeaderElectionDisabled
  }

  def testUncleanLeaderElectionInvalidTopicOverride {
    startBrokers(Seq(configProps1))

    // create topic with an invalid value for unclean leader election
    val topicProps = new Properties()
    topicProps.put("unclean.leader.election.enable", "invalid")

    intercept[IllegalArgumentException] {
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, Map(partitionId -> Seq(brokerId1)), topicProps)
    }
  }

  def verifyUncleanLeaderElectionEnabled {
    // wait until leader is elected
    val leaderIdOpt = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 1000)
    assertTrue("Leader should get elected", leaderIdOpt.isDefined)
    val leaderId = leaderIdOpt.get
    debug("Leader for " + topic  + " is elected to be: %s".format(leaderId))
    assertTrue("Leader id is set to expected value for topic: " + topic, leaderId == brokerId1 || leaderId == brokerId2)

    // the non-leader broker is the follower
    val followerId = if (leaderId == brokerId1) brokerId2 else brokerId1
    debug("Follower for " + topic  + " is: %s".format(followerId))

    produceMessage(topic, "first")
    waitUntilMetadataIsPropagated(servers, topic, partitionId, 1000)
    assertEquals(List("first"), consumeAllMessages(topic))

    // shutdown follower server
    servers.filter(server => server.config.brokerId == followerId).map(server => shutdownServer(server))

    produceMessage(topic, "second")
    assertEquals(List("first", "second"), consumeAllMessages(topic))

    // shutdown leader and then restart follower
    servers.filter(server => server.config.brokerId == leaderId).map(server => shutdownServer(server))
    servers.filter(server => server.config.brokerId == followerId).map(server => server.startup())

    // wait until new leader is (uncleanly) elected
    val newLeaderIdOpt = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 1000, Some(leaderId))
    assertTrue("New leader should get elected", newLeaderIdOpt.isDefined)
    assertEquals(followerId, newLeaderIdOpt.get)

    produceMessage(topic, "third")

    // second message was lost due to unclean election
    assertEquals(List("first", "third"), consumeAllMessages(topic))
  }

  def verifyUncleanLeaderElectionDisabled {
    // wait until leader is elected
    val leaderIdOpt = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 1000)
    assertTrue("Leader should get elected", leaderIdOpt.isDefined)
    val leaderId = leaderIdOpt.get
    debug("Leader for " + topic  + " is elected to be: %s".format(leaderId))
    assertTrue("Leader id is set to expected value for topic: " + topic, leaderId == brokerId1 || leaderId == brokerId2)

    // the non-leader broker is the follower
    val followerId = if (leaderId == brokerId1) brokerId2 else brokerId1
    debug("Follower for " + topic  + " is: %s".format(followerId))

    produceMessage(topic, "first")
    waitUntilMetadataIsPropagated(servers, topic, partitionId, 1000)
    assertEquals(List("first"), consumeAllMessages(topic))

    // shutdown follower server
    servers.filter(server => server.config.brokerId == followerId).map(server => shutdownServer(server))

    produceMessage(topic, "second")
    assertEquals(List("first", "second"), consumeAllMessages(topic))

    // shutdown leader and then restart follower
    servers.filter(server => server.config.brokerId == leaderId).map(server => shutdownServer(server))
    servers.filter(server => server.config.brokerId == followerId).map(server => server.startup())

    // verify that unclean election to non-ISR follower does not occur
    val newLeaderIdOpt = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 1000, Some(leaderId))
    assertTrue("Leader should be defined", newLeaderIdOpt.isDefined)
    assertEquals("No leader should be elected", -1, newLeaderIdOpt.get)

    // message production and consumption should both fail while leader is down
    intercept[FailedToSendMessageException] {
      produceMessage(topic, "third")
    }
    assertEquals(List.empty[String], consumeAllMessages(topic))

    // restart leader temporarily to send a successfully replicated message
    servers.filter(server => server.config.brokerId == leaderId).map(server => server.startup())
    val newLeaderIdOpt2 = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 1000, Some(-1))
    assertTrue("Leader should be defined", newLeaderIdOpt2.isDefined)
    assertEquals("Original leader should be reelected", leaderId, newLeaderIdOpt2.get)
    produceMessage(topic, "third")
    waitUntilMetadataIsPropagated(servers, topic, partitionId, 1000)
    servers.filter(server => server.config.brokerId == leaderId).map(server => shutdownServer(server))

    // verify clean leader transition to ISR follower
    val newLeaderIdOpt3 = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, 1000, Some(leaderId))
    assertTrue("Leader should be defined", newLeaderIdOpt3.isDefined)
    assertEquals("New leader should be elected", followerId, newLeaderIdOpt3.get)

    // verify messages can be consumed from ISR follower that was just promoted to leader
    assertEquals(List("first", "second", "third"), consumeAllMessages(topic))
  }

  private def shutdownServer(server: KafkaServer) = {
    server.shutdown()
    server.awaitShutdown()
  }

  private def produceMessage(topic: String, message: String) = {
    val props = new Properties()
    props.put("request.required.acks", String.valueOf(-1))
	val producer: Producer[String, Array[Byte]] = createProducer(getBrokerListStrFromConfigs(configs),
      new DefaultEncoder(), new StringEncoder(), props)
    producer.send(new KeyedMessage[String, Array[Byte]](topic, topic, message.getBytes))
    producer.close()
  }

  private def consumeAllMessages(topic: String) : List[String] = {
    // use a fresh consumer group every time so that we don't need to mess with disabling auto-commit or
    // resetting the ZK offset
    val consumerProps = createConsumerProperties(zkConnect, "group" + random.nextLong, "id", 1000)
    val consumerConnector = Consumer.create(new ConsumerConfig(consumerProps))
    val messageStream = consumerConnector.createMessageStreams(Map(topic -> 1))(topic).head

    val messages = new MutableList[String]
    val iter = messageStream.iterator
    try {
      while(iter.hasNext()) {
        messages += new String(iter.next.message) // will throw a timeout exception if the message isn't there
      }
    } catch {
      case e: ConsumerTimeoutException =>
        debug("consumer timed out after receiving " + messages.length + " message(s).")
    } finally {
      consumerConnector.shutdown
    }
    messages.toList
  }
}

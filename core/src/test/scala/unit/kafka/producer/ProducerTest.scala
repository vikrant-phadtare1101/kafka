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

package kafka.producer

import org.scalatest.junit.JUnit3Suite
import kafka.consumer.SimpleConsumer
import kafka.message.Message
import kafka.server.{KafkaConfig, KafkaRequestHandler, KafkaServer}
import kafka.zk.ZooKeeperTestHarness
import org.apache.log4j.{Level, Logger}
import org.junit.Assert._
import org.junit.Test
import kafka.utils._
import java.util
import kafka.admin.{AdminUtils, CreateTopicCommand}
import util.Properties
import kafka.api.FetchRequestBuilder
import kafka.common.{KafkaException, ErrorMapping, FailedToSendMessageException}


class ProducerTest extends JUnit3Suite with ZooKeeperTestHarness with Logging{
  private val brokerId1 = 0
  private val brokerId2 = 1
  private val ports = TestUtils.choosePorts(2)
  private val (port1, port2) = (ports(0), ports(1))
  private var server1: KafkaServer = null
  private var server2: KafkaServer = null
  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null
  private val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])

  private val props1 = TestUtils.createBrokerConfig(brokerId1, port1)
  private val config1 = new KafkaConfig(props1) {
    override val hostName = "localhost"
    override val numPartitions = 4
  }
  private val props2 = TestUtils.createBrokerConfig(brokerId2, port2)
  private val config2 = new KafkaConfig(props2) {
    override val hostName = "localhost"
    override val numPartitions = 4
  }

  override def setUp() {
    super.setUp()
    // set up 2 brokers with 4 partitions each
    server1 = TestUtils.createServer(config1)
    server2 = TestUtils.createServer(config2)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", port1.toString)

    consumer1 = new SimpleConsumer("localhost", port1, 1000000, 64*1024)
    consumer2 = new SimpleConsumer("localhost", port2, 100, 64*1024)

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)
  }

  override def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
    server1.shutdown
    server1.awaitShutdown()
    server2.shutdown
    server2.awaitShutdown()
    Utils.rm(server1.config.logDir)
    Utils.rm(server2.config.logDir)
    super.tearDown()
  }


  def testUpdateBrokerPartitionInfo() {
    CreateTopicCommand.createTopic(zkClient, "new-topic", 1, 2)
    assertTrue("Topic new-topic not created after timeout", TestUtils.waitUntilTrue(() =>
      AdminUtils.getTopicMetaDataFromZK(List("new-topic"),
        zkClient).head.errorCode != ErrorMapping.UnknownTopicOrPartitionCode, zookeeper.tickTime))
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, "new-topic", 0, 500)

    val props1 = new util.Properties()
    props1.put("broker.list", "localhost:80,localhost:81")
    props1.put("serializer.class", "kafka.serializer.StringEncoder")
    val producerConfig1 = new ProducerConfig(props1)
    val producer1 = new Producer[String, String](producerConfig1)
    try{
      producer1.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
      fail("Test should fail because the broker list provided are not valid")
    } catch {
      case e: KafkaException =>
      case oe => fail("fails with exception", oe)
    } finally {
      producer1.close()
    }

    val props2 = new util.Properties()
    props2.put("broker.list", "localhost:80," + TestUtils.getBrokerListStrFromConfigs(Seq( config1)))
    props2.put("serializer.class", "kafka.serializer.StringEncoder")
    val producerConfig2= new ProducerConfig(props2)
    val producer2 = new Producer[String, String](producerConfig2)
    try{
      producer2.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
    } catch {
      case e => fail("Should succeed sending the message", e)
    } finally {
      producer2.close()
    }

    val props3 = new util.Properties()
    props3.put("broker.list", TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)))
    props3.put("serializer.class", "kafka.serializer.StringEncoder")
    val producerConfig3 = new ProducerConfig(props3)
    val producer3 = new Producer[String, String](producerConfig3)
    try{
      producer3.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
    } catch {
      case e => fail("Should succeed sending the message", e)
    } finally {
      producer3.close()
    }
  }

  @Test
  def testSendToNewTopic() {
    val props1 = new util.Properties()
    props1.put("serializer.class", "kafka.serializer.StringEncoder")
    props1.put("partitioner.class", "kafka.utils.StaticPartitioner")
    props1.put("broker.list", TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)))
    props1.put("producer.request.required.acks", "2")
    props1.put("producer.request.timeout.ms", "1000")

    val props2 = new util.Properties()
    props2.putAll(props1)
    props2.put("producer.request.required.acks", "3")
    props2.put("producer.request.timeout.ms", "1000")

    val producerConfig1 = new ProducerConfig(props1)
    val producerConfig2 = new ProducerConfig(props2)

    // create topic with 1 partition and await leadership
    CreateTopicCommand.createTopic(zkClient, "new-topic", 1, 2)
    assertTrue("Topic new-topic not created after timeout", TestUtils.waitUntilTrue(() =>
      AdminUtils.getTopicMetaDataFromZK(List("new-topic"),
        zkClient).head.errorCode != ErrorMapping.UnknownTopicOrPartitionCode, zookeeper.tickTime))
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, "new-topic", 0, 500)

    val producer1 = new Producer[String, String](producerConfig1)
    val producer2 = new Producer[String, String](producerConfig2)
    // Available partition ids should be 0.
    producer1.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
    producer1.send(new ProducerData[String, String]("new-topic", "test", Array("test2")))
    // get the leader
    val leaderOpt = ZkUtils.getLeaderForPartition(zkClient, "new-topic", 0)
    assertTrue("Leader for topic new-topic partition 0 should exist", leaderOpt.isDefined)
    val leader = leaderOpt.get

    val messageSet = if(leader == server1.config.brokerId) {
      val response1 = consumer1.fetch(new FetchRequestBuilder().addFetch("new-topic", 0, 0, 10000).build())
      response1.messageSet("new-topic", 0).iterator
    }else {
      val response2 = consumer2.fetch(new FetchRequestBuilder().addFetch("new-topic", 0, 0, 10000).build())
      response2.messageSet("new-topic", 0).iterator
    }
    assertTrue("Message set should have 1 message", messageSet.hasNext)

    assertEquals(new Message("test1".getBytes), messageSet.next.message)
    assertTrue("Message set should have 1 message", messageSet.hasNext)
    assertEquals(new Message("test2".getBytes), messageSet.next.message)
    if (messageSet.hasNext)
      fail("Message set should not have any more messages, but received a message of %s"
            .format(Utils.toString(messageSet.next.message.payload, "UTF-8")))
    producer1.close()

    try {
      producer2.send(new ProducerData[String, String]("new-topic", "test", Array("test2")))
      fail("Should have timed out for 3 acks.")
    }
    catch {
      case se: FailedToSendMessageException => true
      case e => fail("Not expected", e)
    }
    finally {
      producer2.close()
    }
  }


  @Test
  def testSendWithDeadBroker() {
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "kafka.utils.StaticPartitioner")
    props.put("producer.request.timeout.ms", "2000")
//    props.put("producer.request.required.acks", "-1")
    props.put("broker.list", TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)))

    // create topic
    CreateTopicCommand.createTopic(zkClient, "new-topic", 4, 2, "0,0,0,0")
    assertTrue("Topic new-topic not created after timeout", TestUtils.waitUntilTrue(() =>
      AdminUtils.getTopicMetaDataFromZK(List("new-topic"),
        zkClient).head.errorCode != ErrorMapping.UnknownTopicOrPartitionCode, zookeeper.tickTime))
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, "new-topic", 0, 500)
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, "new-topic", 1, 500)
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, "new-topic", 2, 500)
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, "new-topic", 3, 500)

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    try {
      // Available partition ids should be 0, 1, 2 and 3, all lead and hosted only
      // on broker 0
      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
    } catch {
      case e => fail("Unexpected exception: " + e)
    }

    // kill the broker
    server1.shutdown
    server1.awaitShutdown()

    try {
      // These sends should fail since there are no available brokers
      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test1")))
      fail("Should fail since no leader exists for the partition.")
    } catch {
      case e => // success
    }

    // restart server 1
    server1.startup()
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, "new-topic", 0, 500)

    try {
      // cross check if broker 1 got the messages
      val response1 = consumer1.fetch(new FetchRequestBuilder().addFetch("new-topic", 0, 0, 10000).build())
      val messageSet1 = response1.messageSet("new-topic", 0).iterator
      assertTrue("Message set should have 1 message", messageSet1.hasNext)
      assertEquals(new Message("test1".getBytes), messageSet1.next.message)
      assertFalse("Message set should have another message", messageSet1.hasNext)
    } catch {
      case e: Exception => fail("Not expected", e)
    }
    producer.close
  }

  @Test
  def testAsyncSendCanCorrectlyFailWithTimeout() {
    val timeoutMs = 500
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "kafka.utils.StaticPartitioner")
    props.put("producer.request.timeout.ms", String.valueOf(timeoutMs))
    props.put("broker.list", TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)))

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // create topics in ZK
    CreateTopicCommand.createTopic(zkClient, "new-topic", 4, 2, "0:1,0:1,0:1,0:1")
    assertTrue("Topic new-topic not created after timeout", TestUtils.waitUntilTrue(() =>
      AdminUtils.getTopicMetaDataFromZK(List("new-topic"),
        zkClient).head.errorCode != ErrorMapping.UnknownTopicOrPartitionCode, zookeeper.tickTime))
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, "new-topic", 0, 500)

    // do a simple test to make sure plumbing is okay
    try {
      // this message should be assigned to partition 0 whose leader is on broker 0
      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test")))
      // cross check if brokers got the messages
      val response1 = consumer1.fetch(new FetchRequestBuilder().addFetch("new-topic", 0, 0, 10000).build())
      val messageSet1 = response1.messageSet("new-topic", 0).iterator
      assertTrue("Message set should have 1 message", messageSet1.hasNext)
      assertEquals(new Message("test".getBytes), messageSet1.next.message)
    } catch {
      case e => case e: Exception => producer.close; fail("Not expected", e)
    }

    // stop IO threads and request handling, but leave networking operational
    // any requests should be accepted and queue up, but not handled
    server1.requestHandlerPool.shutdown()

    val t1 = SystemTime.milliseconds
    try {
      // this message should be assigned to partition 0 whose leader is on broker 0, but
      // broker 0 will not response within timeoutMs millis.
      producer.send(new ProducerData[String, String]("new-topic", "test", Array("test")))
    } catch {
      case e: FailedToSendMessageException => /* success */
      case e: Exception => fail("Not expected", e)
    } finally {
      producer.close
    }
    val t2 = SystemTime.milliseconds

    // make sure we don't wait fewer than numRetries*timeoutMs milliseconds
    // we do this because the DefaultEventHandler retries a number of times
    assertTrue((t2-t1) >= timeoutMs*config.producerRetries)
  }
}


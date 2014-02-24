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

package kafka.test

import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{ZkUtils, Utils, TestUtils, Logging}
import kafka.zk.ZooKeeperTestHarness
import kafka.admin.AdminUtils
import kafka.consumer.SimpleConsumer
import kafka.api.FetchRequestBuilder
import kafka.message.Message

import org.apache.kafka.clients.producer._

import org.scalatest.junit.JUnit3Suite
import org.junit.Test
import org.junit.Assert._

import java.util.Properties
import java.lang.{Integer, IllegalArgumentException}
import org.apache.log4j.Logger


class ProducerSendTest extends JUnit3Suite with ZooKeeperTestHarness {
  private val brokerId1 = 0
  private val brokerId2 = 1
  private val ports = TestUtils.choosePorts(2)
  private val (port1, port2) = (ports(0), ports(1))
  private var server1: KafkaServer = null
  private var server2: KafkaServer = null
  private var servers = List.empty[KafkaServer]

  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null

  private val props1 = TestUtils.createBrokerConfig(brokerId1, port1)
  private val props2 = TestUtils.createBrokerConfig(brokerId2, port2)
  props1.put("num.partitions", "4")
  props2.put("num.partitions", "4")
  private val config1 = new KafkaConfig(props1)
  private val config2 = new KafkaConfig(props2)

  private val topic = "topic"
  private val numRecords = 100

  override def setUp() {
    super.setUp()
    // set up 2 brokers with 4 partitions each
    server1 = TestUtils.createServer(config1)
    server2 = TestUtils.createServer(config2)
    servers = List(server1,server2)

    // TODO: we need to migrate to new consumers when 0.9 is final
    consumer1 = new SimpleConsumer("localhost", port1, 100, 1024*1024, "")
    consumer2 = new SimpleConsumer("localhost", port2, 100, 1024*1024, "")
  }

  override def tearDown() {
    server1.shutdown
    server2.shutdown
    Utils.rm(server1.config.logDirs)
    Utils.rm(server2.config.logDirs)
    super.tearDown()
  }

  class PrintOffsetCallback extends Callback {
    def onCompletion(metadata: RecordMetadata, exception: Exception) {
      if (exception != null)
        fail("Send callback returns the following exception", exception)
      try {
        System.out.println("The message we just sent is marked as [" + metadata.partition + "] : " + metadata.offset);
      } catch {
        case e: Throwable => fail("Should succeed sending the message", e)
      }
    }
  }

  /**
   * testSendOffset checks the basic send API behavior
   *
   * 1. Send with null key/value/partition-id should be accepted; send with null topic should be rejected.
   * 2. Last message of the non-blocking send should return the correct offset metadata
   */
  @Test
  def testSendOffset() {
    val props = new Properties()
    props.put(ProducerConfig.BROKER_LIST_CONFIG, TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)))
    var producer = new KafkaProducer(props)

    val callback = new PrintOffsetCallback

    try {
      // create topic
      TestUtils.createTopic(zkClient, topic, 1, 2, servers)

      // send a normal record
      val record0 = new ProducerRecord(topic, new Integer(0), "key".getBytes, "value".getBytes)
      val response0 = producer.send(record0, callback)
      assertEquals("Should have offset 0", 0L, response0.get.offset)

      // send a record with null value should be ok
      val record1 = new ProducerRecord(topic, new Integer(0), "key".getBytes, null)
      val response1 = producer.send(record1, callback)
      assertEquals("Should have offset 1", 1L, response1.get.offset)

      // send a record with null key should be ok
      val record2 = new ProducerRecord(topic, new Integer(0), null, "value".getBytes)
      val response2 = producer.send(record2, callback)
      assertEquals("Should have offset 2", 2L, response2.get.offset)

      // send a record with null part id should be ok
      val record3 = new ProducerRecord(topic, null, "key".getBytes, "value".getBytes)
      val response3 = producer.send(record3, callback)
      assertEquals("Should have offset 3", 3L, response3.get.offset)

      // send a record with null topic should fail
      try {
        val record4 = new ProducerRecord(null, new Integer(0), "key".getBytes, "value".getBytes)
        val response4 = producer.send(record4, callback)
        response4.wait
      } catch {
        case iae: IllegalArgumentException => // this is ok
        case e: Throwable => fail("Only expecting IllegalArgumentException", e)
      }

      // non-blocking send a list of records
      for (i <- 1 to numRecords)
        producer.send(record0)

      // check that all messages have been acked via offset
      val response5 = producer.send(record0, callback)
      assertEquals("Should have offset " + (numRecords + 4), numRecords + 4L, response5.get.offset)

    } finally {
      if (producer != null) {
        producer.close()
        producer = null
      }
    }
  }

  /**
   * testClose checks the closing behavior
   *
   * 1. After close() returns, all messages should be sent with correct returned offset metadata
   */
  @Test
  def testClose() {
    val props = new Properties()
    props.put(ProducerConfig.BROKER_LIST_CONFIG, TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)))
    var producer = new KafkaProducer(props)

    try {
      // create topic
      TestUtils.createTopic(zkClient, topic, 1, 2, servers)

      // non-blocking send a list of records
      val record0 = new ProducerRecord(topic, null, "key".getBytes, "value".getBytes)
      for (i <- 1 to numRecords)
        producer.send(record0)
      val response0 = producer.send(record0)

      // close the producer
      producer.close()
      producer = null

      // check that all messages have been acked via offset,
      // this also checks that messages with same key go to the same partition
      assertTrue("The last message should be acked before producer is shutdown", response0.isDone)
      assertEquals("Should have offset " + numRecords, numRecords.toLong, response0.get.offset)

    } finally {
      if (producer != null) {
        producer.close()
        producer = null
      }
    }
  }

  /**
   * testSendToPartition checks the partitioning behavior
   *
   * 1. The specified partition-id should be respected
   */
  @Test
  def testSendToPartition() {
    val props = new Properties()
    props.put(ProducerConfig.BROKER_LIST_CONFIG, TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)))
    props.put(ProducerConfig.REQUIRED_ACKS_CONFIG, "-1")
    var producer = new KafkaProducer(props)

    try {
      // create topic
      val leaders = TestUtils.createTopic(zkClient, topic, 2, 2, servers)

      // make sure leaders exist
      val leader1 = leaders.get(1)
      assertTrue("Leader for topic \"topic\" partition 1 should exist", leader1.isDefined)

      val partition = 1
      val responses =
        for (i <- 0 until numRecords)
        yield producer.send(new ProducerRecord(topic, partition, null, ("value" + i).getBytes))
      val futures = responses.toList
      futures.map(_.wait)
      for (future <- futures)
        assertTrue("Request should have completed", future.isDone)

      // make sure all of them end up in the same partition with increasing offset values
      for ((future, offset) <- futures zip (0 until numRecords)) {
        assertEquals(offset, future.get.offset)
        assertEquals(topic, future.get.topic)
        assertEquals(1, future.get.partition)
      }

      // make sure the fetched messages also respect the partitioning and ordering
      val fetchResponse1 = if(leader1.get == server1.config.brokerId) {
        consumer1.fetch(new FetchRequestBuilder().addFetch(topic, 1, 0, Int.MaxValue).build())
      }else {
        consumer2.fetch(new FetchRequestBuilder().addFetch(topic, 1, 0, Int.MaxValue).build())
      }
      val messageSet1 = fetchResponse1.messageSet(topic, 1).iterator.toBuffer
      assertEquals("Should have fetched " + numRecords + " messages", numRecords, messageSet1.size)

      // TODO: also check topic and partition after they are added in the return messageSet
      for (i <- 0 to numRecords - 1) {
        assertEquals(new Message(bytes = ("value" + (i + 1)).getBytes), messageSet1(i).message)
        assertEquals(i, messageSet1(i).offset)
      }
    } finally {
      if (producer != null) {
        producer.close()
        producer = null
      }
    }
  }

  @Test
  def testAutoCreateTopic() {
    val props = new Properties()
    props.put(ProducerConfig.BROKER_LIST_CONFIG, TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)))
    var producer = new KafkaProducer(props)

    try {
      // Send a message to auto-create the topic
      val record = new ProducerRecord(topic, null, "key".getBytes, "value".getBytes)
      val response = producer.send(record)
      assertEquals("Should have offset 0", 0L, response.get.offset)

      // double check that the topic is created with leader elected
      assertTrue("Topic should already be created with leader", TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 0).isDefined)

    } finally {
      if (producer != null) {
        producer.close()
        producer = null
      }
    }
  }
}
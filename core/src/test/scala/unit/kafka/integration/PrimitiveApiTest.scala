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

import java.nio.ByteBuffer
import junit.framework.Assert._
import kafka.api.{PartitionFetchInfo, FetchRequest, FetchRequestBuilder}
import kafka.server.{KafkaRequestHandler, KafkaConfig}
import java.util.Properties
import kafka.producer.{ProducerData, Producer, ProducerConfig}
import kafka.serializer.StringDecoder
import kafka.message.Message
import kafka.utils.TestUtils
import org.apache.log4j.{Level, Logger}
import org.I0Itec.zkclient.ZkClient
import kafka.zk.ZooKeeperTestHarness
import org.scalatest.junit.JUnit3Suite
import scala.collection._
import kafka.admin.{AdminUtils, CreateTopicCommand}
import kafka.common.{TopicAndPartition, ErrorMapping, UnknownTopicOrPartitionException, OffsetOutOfRangeException}

/**
 * End to end tests of the primitive apis against a local server
 */
class PrimitiveApiTest extends JUnit3Suite with ProducerConsumerTestHarness with ZooKeeperTestHarness {
  
  val port = TestUtils.choosePort
  val props = TestUtils.createBrokerConfig(0, port)
  val config = new KafkaConfig(props) {
    override val flushInterval = 1
  }
  val configs = List(config)
  val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])

  override def setUp() {
    super.setUp
    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)
  }

  override def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)

    super.tearDown
  }

  def testFetchRequestCanProperlySerialize() {
    val request = new FetchRequestBuilder()
      .correlationId(100)
      .clientId("test-client")
      .maxWait(10001)
      .minBytes(4444)
      .addFetch("topic1", 0, 0, 10000)
      .addFetch("topic2", 1, 1024, 9999)
      .addFetch("topic1", 1, 256, 444)
      .build()
    val serializedBuffer = ByteBuffer.allocate(request.sizeInBytes)
    request.writeTo(serializedBuffer)
    serializedBuffer.rewind()
    val deserializedRequest = FetchRequest.readFrom(serializedBuffer)
    assertEquals(request, deserializedRequest)
  }

  def testEmptyFetchRequest() {
    val partitionRequests = immutable.Map[TopicAndPartition, PartitionFetchInfo]()
    val request = new FetchRequest(requestInfo = partitionRequests)
    val fetched = consumer.fetch(request)
    assertTrue(!fetched.hasError && fetched.data.size == 0)
  }

  def testDefaultEncoderProducerAndFetch() {
    val topic = "test-topic"
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("broker.list", TestUtils.getBrokerListStrFromConfigs(configs))
    val config = new ProducerConfig(props)

    val stringProducer1 = new Producer[String, String](config)
    stringProducer1.send(new ProducerData[String, String](topic, Array("test-message")))

    val replica = servers.head.replicaManager.getReplica(topic, 0).get
    assertTrue("HighWatermark should equal logEndOffset with just 1 replica",
               replica.logEndOffset > 0 && replica.logEndOffset == replica.highWatermark)

    val request = new FetchRequestBuilder()
      .correlationId(100)
      .clientId("test-client")
      .addFetch(topic, 0, 0, 10000)
      .build()
    val fetched = consumer.fetch(request)
    assertEquals("Returned correlationId doesn't match that in request.", 100, fetched.correlationId)

    val messageSet = fetched.messageSet(topic, 0)
    assertTrue(messageSet.iterator.hasNext)

    val fetchedMessageAndOffset = messageSet.head
    val stringDecoder = new StringDecoder
    val fetchedStringMessage = stringDecoder.toEvent(fetchedMessageAndOffset.message)
    assertEquals("test-message", fetchedStringMessage)
  }

  def testDefaultEncoderProducerAndFetchWithCompression() {
    val topic = "test-topic"
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("broker.list", TestUtils.getBrokerListStrFromConfigs(configs))
    props.put("compression", "true")
    val config = new ProducerConfig(props)

    val stringProducer1 = new Producer[String, String](config)
    stringProducer1.send(new ProducerData[String, String](topic, Array("test-message")))

    var fetched = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
    val messageSet = fetched.messageSet(topic, 0)
    assertTrue(messageSet.iterator.hasNext)

    val fetchedMessageAndOffset = messageSet.head
    val stringDecoder = new StringDecoder
    val fetchedStringMessage = stringDecoder.toEvent(fetchedMessageAndOffset.message)
    assertEquals("test-message", fetchedStringMessage)
  }

  def testProduceAndMultiFetch() {
    createSimpleTopicsAndAwaitLeader(zkClient, List("test1", "test2", "test3", "test4"), config.brokerId)

    // send some messages
    val topics = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    {
      val messages = new mutable.HashMap[String, Seq[Message]]
      val builder = new FetchRequestBuilder()
      for( (topic, partition) <- topics) {
        val messageList = List(new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
        val producerData = new ProducerData[String, Message](topic, topic, messageList)
        messages += topic -> messageList
        producer.send(producerData)
        builder.addFetch(topic, partition, 0, 10000)
    }

      // wait a bit for produced message to be available
      val request = builder.build()
      val response = consumer.fetch(request)
      for( (topic, partition) <- topics) {
        val fetched = response.messageSet(topic, partition)
        TestUtils.checkEquals(messages(topic).iterator, fetched.map(messageAndOffset => messageAndOffset.message).iterator)
      }
    }

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    {
      // send some invalid offsets
      val builder = new FetchRequestBuilder()
      for((topic, partition) <- topics)
        builder.addFetch(topic, partition, -1, 10000)

      try {
        val request = builder.build()
        val response = consumer.fetch(request)
        response.data.values.foreach(pdata => ErrorMapping.maybeThrowException(pdata.error))
        fail("Expected exception when fetching message with invalid offset")
      } catch {
        case e: OffsetOutOfRangeException => "this is good"
      }
    }

    {
      // send some invalid partitions
      val builder = new FetchRequestBuilder()
      for((topic, partition) <- topics)
        builder.addFetch(topic, -1, 0, 10000)

      try {
        val request = builder.build()
        val response = consumer.fetch(request)
        response.data.values.foreach(pdata => ErrorMapping.maybeThrowException(pdata.error))
        fail("Expected exception when fetching message with invalid partition")
      } catch {
        case e: UnknownTopicOrPartitionException => "this is good"
      }
    }

    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def testProduceAndMultiFetchWithCompression() {
    createSimpleTopicsAndAwaitLeader(zkClient, List("test1", "test2", "test3", "test4"), config.brokerId)

    // send some messages
    val topics = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    {
      val messages = new mutable.HashMap[String, Seq[Message]]
      val builder = new FetchRequestBuilder()
      for( (topic, partition) <- topics) {
        val messageList = List(new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
        val producerData = new ProducerData[String, Message](topic, topic, messageList)
        messages += topic -> messageList
        producer.send(producerData)
        builder.addFetch(topic, partition, 0, 10000)
      }

      // wait a bit for produced message to be available
      val request = builder.build()
      val response = consumer.fetch(request)
      for( (topic, partition) <- topics) {
        val fetched = response.messageSet(topic, partition)
        TestUtils.checkEquals(messages(topic).iterator, fetched.map(messageAndOffset => messageAndOffset.message).iterator)
      }
    }

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    {
      // send some invalid offsets
      val builder = new FetchRequestBuilder()
      for( (topic, partition) <- topics)
        builder.addFetch(topic, partition, -1, 10000)

      try {
        val request = builder.build()
        val response = consumer.fetch(request)
        response.data.values.foreach(pdata => ErrorMapping.maybeThrowException(pdata.error))
        fail("Expected exception when fetching message with invalid offset")
      } catch {
        case e: OffsetOutOfRangeException => "this is good"
      }
    }

    {
      // send some invalid partitions
      val builder = new FetchRequestBuilder()
      for( (topic, _) <- topics)
        builder.addFetch(topic, -1, 0, 10000)

      try {
        val request = builder.build()
        val response = consumer.fetch(request)
        response.data.values.foreach(pdata => ErrorMapping.maybeThrowException(pdata.error))
        fail("Expected exception when fetching message with invalid partition")
      } catch {
        case e: UnknownTopicOrPartitionException => "this is good"
      }
    }

    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def testMultiProduce() {
    createSimpleTopicsAndAwaitLeader(zkClient, List("test1", "test2", "test3", "test4"), config.brokerId)

    // send some messages
    val topics = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    val messages = new mutable.HashMap[String, Seq[Message]]
    val builder = new FetchRequestBuilder()
    var produceList: List[ProducerData[String, Message]] = Nil
    for( (topic, partition) <- topics) {
      val messageList = List(new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
      val producerData = new ProducerData[String, Message](topic, topic, messageList)
      messages += topic -> messageList
      producer.send(producerData)
      builder.addFetch(topic, partition, 0, 10000)
    }
    producer.send(produceList: _*)

    // wait a bit for produced message to be available
    val request = builder.build()
    val response = consumer.fetch(request)
    for( (topic, partition) <- topics) {
      val fetched = response.messageSet(topic, partition)
      TestUtils.checkEquals(messages(topic).iterator, fetched.map(messageAndOffset => messageAndOffset.message).iterator)
    }
  }

  def testMultiProduceWithCompression() {
    // send some messages
    val topics = List(("test4", 0), ("test1", 0), ("test2", 0), ("test3", 0));
    val messages = new mutable.HashMap[String, Seq[Message]]
    val builder = new FetchRequestBuilder()
    var produceList: List[ProducerData[String, Message]] = Nil
    for( (topic, partition) <- topics) {
      val messageList = List(new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
      val producerData = new ProducerData[String, Message](topic, topic, messageList)
      messages += topic -> messageList
      producer.send(producerData)
      builder.addFetch(topic, partition, 0, 10000)
    }
    producer.send(produceList: _*)

    // wait a bit for produced message to be available
    val request = builder.build()
    val response = consumer.fetch(request)
    for( (topic, partition) <- topics) {
      val fetched = response.messageSet(topic, 0)
      TestUtils.checkEquals(messages(topic).iterator, fetched.map(messageAndOffset => messageAndOffset.message).iterator)
    }
  }

  def testConsumerEmptyTopic() {
    val newTopic = "new-topic"
    CreateTopicCommand.createTopic(zkClient, newTopic, 1, 1, config.brokerId.toString)
    assertTrue("Topic new-topic not created after timeout", TestUtils.waitUntilTrue(() =>
      AdminUtils.getTopicMetaDataFromZK(List(newTopic),
        zkClient).head.errorCode != ErrorMapping.UnknownTopicOrPartitionCode, zookeeper.tickTime))
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, newTopic, 0, 500)
    val fetchResponse = consumer.fetch(new FetchRequestBuilder().addFetch(newTopic, 0, 0, 10000).build())
    assertFalse(fetchResponse.messageSet(newTopic, 0).iterator.hasNext)
  }

  /**
   * For testing purposes, just create these topics each with one partition and one replica for
   * which the provided broker should the leader for.  Create and wait for broker to lead.  Simple.
   */
  def createSimpleTopicsAndAwaitLeader(zkClient: ZkClient, topics: Seq[String], brokerId: Int) {
    for( topic <- topics ) {
      CreateTopicCommand.createTopic(zkClient, topic, 1, 1, brokerId.toString)
      TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500)
    }
  }
}

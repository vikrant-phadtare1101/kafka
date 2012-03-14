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

import java.util.Properties
import junit.framework.Assert
import kafka.admin.CreateTopicCommand
import kafka.common.{ErrorMapping, MessageSizeTooLargeException}
import kafka.integration.KafkaServerTestHarness
import kafka.message.{NoCompressionCodec, Message, ByteBufferMessageSet}
import kafka.server.KafkaConfig
import kafka.utils.{TestZKUtils, SystemTime, TestUtils}
import org.junit.Test
import org.scalatest.junit.JUnit3Suite

class SyncProducerTest extends JUnit3Suite with KafkaServerTestHarness {
  private var messageBytes =  new Array[Byte](2);
  val configs = List(new KafkaConfig(TestUtils.createBrokerConfigs(1).head))
  val zookeeperConnect = TestZKUtils.zookeeperConnect

  @Test
  def testReachableServer() {
    val server = servers.head
    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", server.socketServer.port.toString)
    props.put("buffer.size", "102400")
    props.put("connect.timeout.ms", "500")
    props.put("reconnect.interval", "1000")
    val producer = new SyncProducer(new SyncProducerConfig(props))
    val firstStart = SystemTime.milliseconds
    try {
      val response = producer.send(TestUtils.produceRequest("test", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = new Message(messageBytes))))
      Assert.assertNotNull(response)
    } catch {
      case e: Exception => Assert.fail("Unexpected failure sending message to broker. " + e.getMessage)
    }
    val firstEnd = SystemTime.milliseconds
    Assert.assertTrue((firstEnd-firstStart) < 500)
    val secondStart = SystemTime.milliseconds
    try {
      val response = producer.send(TestUtils.produceRequest("test", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = new Message(messageBytes))))
      Assert.assertNotNull(response)
    } catch {
      case e: Exception => Assert.fail("Unexpected failure sending message to broker. " + e.getMessage)
    }
    val secondEnd = SystemTime.milliseconds
    Assert.assertTrue((secondEnd-secondStart) < 500)
    try {
      val response = producer.send(TestUtils.produceRequest("test", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = new Message(messageBytes))))
      Assert.assertNotNull(response)
    } catch {
      case e: Exception => Assert.fail("Unexpected failure sending message to broker. " + e.getMessage)
    }
  }

  @Test
  def testMessageSizeTooLarge() {
    val server = servers.head
    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", server.socketServer.port.toString)
    props.put("buffer.size", "102400")
    props.put("connect.timeout.ms", "300")
    props.put("reconnect.interval", "500")
    props.put("max.message.size", "100")
    val producer = new SyncProducer(new SyncProducerConfig(props))
    val bytes = new Array[Byte](101)
    try {
      producer.send(TestUtils.produceRequest("test", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, messages = new Message(bytes))))
      Assert.fail("Message was too large to send, SyncProducer should have thrown exception.")
    } catch {
      case e: MessageSizeTooLargeException => /* success */
    }
  }

  @Test
  def testProduceBlocksWhenRequired() {
    // TODO: this will need to change with kafka-44
    val server = servers.head
    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", server.socketServer.port.toString)
    props.put("buffer.size", "102400")
    props.put("connect.timeout.ms", "300")
    props.put("reconnect.interval", "500")
    props.put("max.message.size", "100")

    val producer = new SyncProducer(new SyncProducerConfig(props))
    val messages = new ByteBufferMessageSet(NoCompressionCodec, new Message(messageBytes))

    // #1 - test that we get an error when partition does not belong to broker in response
    val request = TestUtils.produceRequestWithAcks(Array("topic1", "topic2", "topic3"), Array(0), messages)
    val response = producer.send(request)

    Assert.assertEquals(request.correlationId, response.correlationId)
    Assert.assertEquals(response.errors.length, response.offsets.length)
    Assert.assertEquals(3, response.errors.length)
    response.errors.foreach(Assert.assertEquals(ErrorMapping.WrongPartitionCode.toShort, _))
    response.offsets.foreach(Assert.assertEquals(-1L, _))

    // #2 - test that we get correct offsets when partition is owner by broker
    val zkClient = zookeeper.client
    CreateTopicCommand.createTopic(zkClient, "topic1", 1, 1)
    CreateTopicCommand.createTopic(zkClient, "topic3", 1, 1)

    val response2 = producer.send(request)
    Assert.assertEquals(request.correlationId, response2.correlationId)
    Assert.assertEquals(response2.errors.length, response2.offsets.length)
    Assert.assertEquals(3, response2.errors.length)

    // the first and last message should have been accepted by broker
    Assert.assertEquals(0, response2.errors(0))
    Assert.assertEquals(0, response2.errors(2))
    Assert.assertEquals(messages.sizeInBytes, response2.offsets(0))
    Assert.assertEquals(messages.sizeInBytes, response2.offsets(2))

    // the middle message should have been rejected because broker doesn't lead partition
    Assert.assertEquals(ErrorMapping.WrongPartitionCode.toShort, response2.errors(1))
    Assert.assertEquals(-1, response2.offsets(1))
  }
}

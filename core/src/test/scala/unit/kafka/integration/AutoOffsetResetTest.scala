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

import junit.framework.Assert._
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.atomic.AtomicInteger
import kafka.utils.{ZKGroupTopicDirs, Logging}
import kafka.consumer.{ConsumerTimeoutException, ConsumerConfig, ConsumerConnector, Consumer}
import kafka.server._
import org.apache.log4j.{Level, Logger}
import org.scalatest.junit.JUnit3Suite
import kafka.utils.TestUtils
import kafka.message.Message
import kafka.producer.{Producer, ProducerData}

class AutoOffsetResetTest extends JUnit3Suite with KafkaServerTestHarness with Logging {

  val topic = "test_topic"
  val group = "default_group"
  val testConsumer = "consumer"
  val BrokerPort = 9892
  val configs = List(new KafkaConfig(TestUtils.createBrokerConfig(0, BrokerPort)))
  val NumMessages = 10
  val LargeOffset = 10000
  val SmallOffset = -1
  
  val requestHandlerLogger = Logger.getLogger(classOf[kafka.server.KafkaRequestHandler])

  override def setUp() {
    super.setUp()

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)
  }

  override def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
    super.tearDown
  }
  
  def testResetToEarliestWhenOffsetTooHigh() = 
    assertEquals(NumMessages, resetAndConsume(NumMessages, "smallest", LargeOffset))
  
  def testResetToEarliestWhenOffsetTooLow() =
    assertEquals(NumMessages, resetAndConsume(NumMessages, "smallest", SmallOffset))
    
  def testResetToLatestWhenOffsetTooHigh() = 
    assertEquals(0, resetAndConsume(NumMessages, "largest", LargeOffset))
    
  def testResetToLatestWhenOffsetTooLow() = 
    assertEquals(0, resetAndConsume(NumMessages, "largest", SmallOffset))
  
  /* Produce the given number of messages, create a consumer with the given offset policy, 
   * then reset the offset to the given value and consume until we get no new messages. 
   * Returns the count of messages received.
   */
  def resetAndConsume(numMessages: Int, resetTo: String, offset: Long): Int = {
    val producer: Producer[String, Message] = TestUtils.createProducer(zkConnect)

    for(i <- 0 until numMessages)
      producer.send(new ProducerData[String, Message](topic, topic, new Message("test".getBytes())))

    // update offset in zookeeper for consumer to jump "forward" in time
    val dirs = new ZKGroupTopicDirs(group, topic)
    var consumerProps = TestUtils.createConsumerProperties(zkConnect, group, testConsumer)
    consumerProps.put("autooffset.reset", resetTo)
    consumerProps.put("consumer.timeout.ms", "2000")
    consumerProps.put("max.fetch.wait.ms", "0")
    val consumerConfig = new ConsumerConfig(consumerProps)

    TestUtils.updateConsumerOffset(consumerConfig, dirs.consumerOffsetDir + "/" + "0", offset)
    info("Updated consumer offset to " + offset)
    
    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val messageStream = consumerConnector.createMessageStreams(Map(topic -> 1))(topic).head

    var received = 0
    val iter = messageStream.iterator
    try {
      for (i <- 0 until numMessages) {
        iter.next // will throw a timeout exception if the message isn't there
        received += 1
      }
    } catch {
      case e: ConsumerTimeoutException => 
        info("consumer timed out after receiving " + received + " messages.")
    }
    consumerConnector.shutdown
    received
  }
  
}

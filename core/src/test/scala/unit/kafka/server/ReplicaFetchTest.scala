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
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.TestUtils._
import kafka.producer.ProducerData
import kafka.serializer.StringEncoder
import kafka.admin.CreateTopicCommand
import kafka.utils.TestUtils
import junit.framework.Assert._

class ReplicaFetchTest extends JUnit3Suite with ZooKeeperTestHarness  {
  val props = createBrokerConfigs(2)
  val configs = props.map(p => new KafkaConfig(p) { override val flushInterval = 1})
  var brokers: Seq[KafkaServer] = null
  val topic1 = "foo"
  val topic2 = "bar"

  override def setUp() {
    super.setUp()
    brokers = configs.map(config => TestUtils.createServer(config))
  }

  override def tearDown() {
    brokers.foreach(_.shutdown())
    super.tearDown()
  }

  def testReplicaFetcherThread() {
    val partition = 0
    val testMessageList1 = List("test1", "test2", "test3", "test4")
    val testMessageList2 = List("test5", "test6", "test7", "test8")

    // create a topic and partition and await leadership
    for (topic <- List(topic1,topic2)) {
      CreateTopicCommand.createTopic(zkClient, topic, 1, 2, configs.map(c => c.brokerId).mkString(":"))
      TestUtils.waitUntilLiveLeaderIsElected(zkClient, topic, 0, 1000)
    }

    // send test messages to leader
    val producer = TestUtils.createProducer[String, String](zkConnect, new StringEncoder)
    producer.send(new ProducerData[String, String](topic1, testMessageList1),
                  new ProducerData[String, String](topic2, testMessageList2))
    producer.close()

    def condition(): Boolean = {
      var result = true
      for (topic <- List(topic1, topic2)) {
        val expectedOffset = brokers.head.getLogManager().getLog(topic, partition).get.logEndOffset
        result = result && expectedOffset > 0 && brokers.foldLeft(true) { (total, item) => total &&
          (expectedOffset == item.getLogManager().getLog(topic, partition).get.logEndOffset) }
      }
      result
    }
    assertTrue("broker logs should be identical", waitUntilTrue(condition, 6000))
  }
}
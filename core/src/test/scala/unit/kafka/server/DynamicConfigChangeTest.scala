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

import java.util.Properties

import junit.framework.Assert._
import org.junit.Test
import kafka.integration.KafkaServerTestHarness
import kafka.utils._
import kafka.common._
import kafka.log.LogConfig
import kafka.admin.{AdminOperationException, AdminUtils}
import org.scalatest.junit.JUnit3Suite

class DynamicConfigChangeTest extends JUnit3Suite with KafkaServerTestHarness {
  def generateConfigs() = List(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))

  @Test
  def testConfigChange() {
    val oldVal: java.lang.Long = 100000
    val newVal: java.lang.Long = 200000
    val tp = TopicAndPartition("test", 0)
    val logProps = new Properties()
    logProps.put(LogConfig.FlushMessagesProp, oldVal.toString)
    AdminUtils.createTopic(zkClient, tp.topic, 1, 1, logProps)
    TestUtils.retry(10000) {
      val logOpt = this.servers(0).logManager.getLog(tp)
      assertTrue(logOpt.isDefined)
      assertEquals(oldVal, logOpt.get.config.flushInterval)
    }
    logProps.put(LogConfig.FlushMessagesProp, newVal.toString)
    AdminUtils.changeTopicConfig(zkClient, tp.topic, logProps)
    TestUtils.retry(10000) {
      assertEquals(newVal, this.servers(0).logManager.getLog(tp).get.config.flushInterval)
    }
  }

  @Test
  def testConfigChangeOnNonExistingTopic() {
    val topic = TestUtils.tempTopic
    try {
      val logProps = new Properties()
      logProps.put(LogConfig.FlushMessagesProp, 10000: java.lang.Integer)
      AdminUtils.changeTopicConfig(zkClient, topic, logProps)
      fail("Should fail with AdminOperationException for topic doesn't exist")
    } catch {
      case e: AdminOperationException => // expected
    }
  }

}
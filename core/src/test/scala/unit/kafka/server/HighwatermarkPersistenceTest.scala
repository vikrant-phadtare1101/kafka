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

import kafka.log._
import java.io.File

import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Utils
import org.easymock.EasyMock
import org.junit._
import org.junit.Assert._
import kafka.cluster.Replica
import kafka.utils.{KafkaScheduler, MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.common.TopicPartition

class HighwatermarkPersistenceTest {

  val configs = TestUtils.createBrokerConfigs(2, TestUtils.MockZkConnect).map(KafkaConfig.fromProps)
  val topic = "foo"
  val zkClient: KafkaZkClient = EasyMock.createMock(classOf[KafkaZkClient])
  val logManagers = configs map { config =>
    TestUtils.createLogManager(
      logDirs = config.logDirs.map(new File(_)),
      cleanerConfig = CleanerConfig())
  }

  val logDirFailureChannels = configs map { config =>
    new LogDirFailureChannel(config.logDirs.size)
  }

  @After
  def teardown() {
    for (manager <- logManagers; dir <- manager.liveLogDirs)
      Utils.delete(dir)
  }

  @Test
  def testHighWatermarkPersistenceSinglePartition() {
    // mock zkclient
    EasyMock.replay(zkClient)

    // create kafka scheduler
    val scheduler = new KafkaScheduler(2)
    scheduler.startup
    val metrics = new Metrics
    val time = new MockTime
    // create replica manager
    val replicaManager = new ReplicaManager(configs.head, metrics, time, zkClient, scheduler,
      logManagers.head, new AtomicBoolean(false), QuotaFactory.instantiate(configs.head, metrics, time, ""),
      new BrokerTopicStats, new MetadataCache(configs.head.brokerId), logDirFailureChannels.head)
    replicaManager.startup()
    try {
      replicaManager.checkpointHighWatermarks()
      var fooPartition0Hw = hwmFor(replicaManager, topic, 0)
      assertEquals(0L, fooPartition0Hw)
      val tp0 = new TopicPartition(topic, 0)
      val partition0 = replicaManager.createPartition(tp0)
      // create leader and follower replicas
      val log0 = logManagers.head.getOrCreateLog(new TopicPartition(topic, 0), LogConfig())
      partition0.setLog(log0, isFutureLog = false)
      val followerReplicaPartition0 = new Replica(configs.last.brokerId, tp0)
      partition0.addReplicaIfNotExists(followerReplicaPartition0)
      replicaManager.checkpointHighWatermarks()
      fooPartition0Hw = hwmFor(replicaManager, topic, 0)
      assertEquals(log0.highWatermark, fooPartition0Hw)
      // set the high watermark for local replica
      partition0.localLogOrException.highWatermark = 5L
      replicaManager.checkpointHighWatermarks()
      fooPartition0Hw = hwmFor(replicaManager, topic, 0)
      assertEquals(log0.highWatermark, fooPartition0Hw)
      EasyMock.verify(zkClient)
    } finally {
      // shutdown the replica manager upon test completion
      replicaManager.shutdown(false)
      metrics.close()
      scheduler.shutdown()
    }
  }

  @Test
  def testHighWatermarkPersistenceMultiplePartitions() {
    val topic1 = "foo1"
    val topic2 = "foo2"
    // mock zkclient
    EasyMock.replay(zkClient)
    // create kafka scheduler
    val scheduler = new KafkaScheduler(2)
    scheduler.startup
    val metrics = new Metrics
    val time = new MockTime
    // create replica manager
    val replicaManager = new ReplicaManager(configs.head, metrics, time, zkClient,
      scheduler, logManagers.head, new AtomicBoolean(false), QuotaFactory.instantiate(configs.head, metrics, time, ""),
      new BrokerTopicStats, new MetadataCache(configs.head.brokerId), logDirFailureChannels.head)
    replicaManager.startup()
    try {
      replicaManager.checkpointHighWatermarks()
      var topic1Partition0Hw = hwmFor(replicaManager, topic1, 0)
      assertEquals(0L, topic1Partition0Hw)
      val t1p0 = new TopicPartition(topic1, 0)
      val topic1Partition0 = replicaManager.createPartition(t1p0)
      // create leader log
      val topic1Log0 = logManagers.head.getOrCreateLog(t1p0, LogConfig())
      // create a local replica for topic1
      topic1Partition0.setLog(topic1Log0, isFutureLog = false)
      replicaManager.checkpointHighWatermarks()
      topic1Partition0Hw = hwmFor(replicaManager, topic1, 0)
      assertEquals(topic1Log0.highWatermark, topic1Partition0Hw)
      // set the high watermark for local replica
      topic1Partition0.localLogOrException.highWatermark = 5L
      replicaManager.checkpointHighWatermarks()
      topic1Partition0Hw = hwmFor(replicaManager, topic1, 0)
      assertEquals(5L, topic1Log0.highWatermark)
      assertEquals(5L, topic1Partition0Hw)
      // add another partition and set highwatermark
      val t2p0 = new TopicPartition(topic2, 0)
      val topic2Partition0 = replicaManager.createPartition(t2p0)
      // create leader log
      val topic2Log0 = logManagers.head.getOrCreateLog(t2p0, LogConfig())
      // create a local replica for topic2
      topic2Partition0.setLog(topic2Log0, isFutureLog = false)
      replicaManager.checkpointHighWatermarks()
      var topic2Partition0Hw = hwmFor(replicaManager, topic2, 0)
      assertEquals(topic2Log0.highWatermark, topic2Partition0Hw)
      // set the highwatermark for local replica
      topic2Partition0.localLogOrException.highWatermark = 15L
      assertEquals(15L, topic2Log0.highWatermark)
      // change the highwatermark for topic1
      topic1Partition0.localLogOrException.highWatermark = 10L
      assertEquals(10L, topic1Log0.highWatermark)
      replicaManager.checkpointHighWatermarks()
      // verify checkpointed hw for topic 2
      topic2Partition0Hw = hwmFor(replicaManager, topic2, 0)
      assertEquals(15L, topic2Partition0Hw)
      // verify checkpointed hw for topic 1
      topic1Partition0Hw = hwmFor(replicaManager, topic1, 0)
      assertEquals(10L, topic1Partition0Hw)
      EasyMock.verify(zkClient)
    } finally {
      // shutdown the replica manager upon test completion
      replicaManager.shutdown(false)
      metrics.close()
      scheduler.shutdown()
    }
  }

  def hwmFor(replicaManager: ReplicaManager, topic: String, partition: Int): Long = {
    replicaManager.highWatermarkCheckpoints(new File(replicaManager.config.logDirs.head).getAbsolutePath).read.getOrElse(
      new TopicPartition(topic, partition), 0L)
  }
}

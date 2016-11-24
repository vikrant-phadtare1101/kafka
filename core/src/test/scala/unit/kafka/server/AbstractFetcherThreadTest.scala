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

import com.yammer.metrics.Metrics
import kafka.cluster.BrokerEndPoint
import kafka.message.{ByteBufferMessageSet, Message, NoCompressionCodec}
import kafka.server.AbstractFetcherThread.{FetchRequest, PartitionData}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.Utils
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.{Before, Test}

import scala.collection.JavaConverters._
import scala.collection.{mutable, Map}

class AbstractFetcherThreadTest {

  @Before
  def cleanMetricRegistry(): Unit = {
    for (metricName <- Metrics.defaultRegistry().allMetrics().keySet().asScala)
      Metrics.defaultRegistry().removeMetric(metricName)
  }

  @Test
  def testMetricsRemovedOnShutdown() {
    val partition = new TopicPartition("topic", 0)
    val fetcherThread = new DummyFetcherThread("dummy", "client", new BrokerEndPoint(0, "localhost", 9092))

    fetcherThread.start()

    // add one partition to create the consumer lag metric
    fetcherThread.addPartitions(Map(partition -> 0L))

    // wait until all fetcher metrics are present
    TestUtils.waitUntilTrue(() =>
      allMetricsNames == Set(FetcherMetrics.BytesPerSec, FetcherMetrics.RequestsPerSec, FetcherMetrics.ConsumerLag),
      "Failed waiting for all fetcher metrics to be registered")

    fetcherThread.shutdown()

    // after shutdown, they should be gone
    assertTrue(Metrics.defaultRegistry().allMetrics().isEmpty)
  }

  @Test
  def testConsumerLagRemovedWithPartition() {
    val partition = new TopicPartition("topic", 0)
    val fetcherThread = new DummyFetcherThread("dummy", "client", new BrokerEndPoint(0, "localhost", 9092))

    fetcherThread.start()

    // add one partition to create the consumer lag metric
    fetcherThread.addPartitions(Map(partition -> 0L))

    // wait until lag metric is present
    TestUtils.waitUntilTrue(() => allMetricsNames(FetcherMetrics.ConsumerLag),
      "Failed waiting for consumer lag metric")

    // remove the partition to simulate leader migration
    fetcherThread.removePartitions(Set(partition))

    // the lag metric should now be gone
    assertFalse(allMetricsNames(FetcherMetrics.ConsumerLag))

    fetcherThread.shutdown()
  }

  private def allMetricsNames = Metrics.defaultRegistry().allMetrics().asScala.keySet.map(_.getName)

  class DummyFetchRequest(val offsets: collection.Map[TopicPartition, Long]) extends FetchRequest {
    override def isEmpty: Boolean = offsets.isEmpty

    override def offset(topicAndPartition: TopicPartition): Long = offsets(topicAndPartition)
  }

  class TestPartitionData(byteBufferMessageSet: ByteBufferMessageSet) extends PartitionData {
    override def errorCode: Short = Errors.NONE.code

    override def toByteBufferMessageSet: ByteBufferMessageSet = byteBufferMessageSet

    override def highWatermark: Long = 0L

    override def exception: Option[Throwable] = None
  }

  class DummyFetcherThread(name: String,
                           clientId: String,
                           sourceBroker: BrokerEndPoint,
                           fetchBackOffMs: Int = 0)
    extends AbstractFetcherThread(name, clientId, sourceBroker, fetchBackOffMs) {

    type REQ = DummyFetchRequest
    type PD = PartitionData

    override def processPartitionData(topicAndPartition: TopicPartition,
                                      fetchOffset: Long,
                                      partitionData: PartitionData): Unit = {}

    override def handleOffsetOutOfRange(topicAndPartition: TopicPartition): Long = 0L

    override def handlePartitionsWithErrors(partitions: Iterable[TopicPartition]): Unit = {}

    override protected def fetch(fetchRequest: DummyFetchRequest): Seq[(TopicPartition, TestPartitionData)] =
      fetchRequest.offsets.mapValues(_ => new TestPartitionData(new ByteBufferMessageSet())).toSeq

    override protected def buildFetchRequest(partitionMap: collection.Seq[(TopicPartition, PartitionFetchState)]): DummyFetchRequest =
      new DummyFetchRequest(partitionMap.map { case (k, v) => (k, v.offset) }.toMap)
  }


  @Test
  def testFetchRequestCorruptedMessageException() {
    val partition = new TopicPartition("topic", 0)
    val fetcherThread = new CorruptingFetcherThread("test", "client", new BrokerEndPoint(0, "localhost", 9092),
      fetchBackOffMs = 1)

    fetcherThread.start()

    // Add one partition for fetching
    fetcherThread.addPartitions(Map(partition -> 0L))

    // Wait until fetcherThread finishes the work
    TestUtils.waitUntilTrue(() => fetcherThread.fetchCount > 3, "Failed waiting for fetcherThread tp finish the work")

    fetcherThread.shutdown()

    // The fetcherThread should have fetched two normal messages
    assertTrue(fetcherThread.logEndOffset == 2)
  }

  class CorruptingFetcherThread(name: String,
                                clientId: String,
                                sourceBroker: BrokerEndPoint,
                                fetchBackOffMs: Int = 0)
    extends DummyFetcherThread(name, clientId, sourceBroker, fetchBackOffMs) {

    @volatile var logEndOffset = 0L
    @volatile var fetchCount = 0

    private val normalPartitionDataSet = List(
      new TestPartitionData(new ByteBufferMessageSet(NoCompressionCodec, Seq(0L), new Message("hello".getBytes))),
      new TestPartitionData(new ByteBufferMessageSet(NoCompressionCodec, Seq(1L), new Message("hello".getBytes)))
    )

    override def processPartitionData(topicAndPartition: TopicPartition,
                                      fetchOffset: Long,
                                      partitionData: PartitionData): Unit = {
      // Throw exception if the fetchOffset does not match the fetcherThread partition state
      if (fetchOffset != logEndOffset)
        throw new RuntimeException(
          "Offset mismatch for partition %s: fetched offset = %d, log end offset = %d."
            .format(topicAndPartition, fetchOffset, logEndOffset))

      // Now check message's crc
      val messages = partitionData.toByteBufferMessageSet
      for (messageAndOffset <- messages.shallowIterator) {
        messageAndOffset.message.ensureValid()
        logEndOffset = messageAndOffset.nextOffset
      }
    }

    override protected def fetch(fetchRequest: DummyFetchRequest): Seq[(TopicPartition, TestPartitionData)] = {
      fetchCount += 1
      // Set the first fetch to get a corrupted message
      if (fetchCount == 1) {
        val corruptedMessage = new Message("hello".getBytes)
        val badChecksum = (corruptedMessage.checksum + 1 % Int.MaxValue).toInt
        // Garble checksum
        Utils.writeUnsignedInt(corruptedMessage.buffer, Message.CrcOffset, badChecksum)
        val byteBufferMessageSet = new ByteBufferMessageSet(NoCompressionCodec, corruptedMessage)
        fetchRequest.offsets.mapValues(_ => new TestPartitionData(byteBufferMessageSet)).toSeq
      } else
      // Then, the following fetches get the normal data
        fetchRequest.offsets.mapValues(v => normalPartitionDataSet(v.toInt)).toSeq
    }

    override protected def buildFetchRequest(partitionMap: collection.Seq[(TopicPartition, PartitionFetchState)]): DummyFetchRequest = {
      val requestMap = new mutable.HashMap[TopicPartition, Long]
      partitionMap.foreach { case (topicPartition, partitionFetchState) =>
        // Add backoff delay check
        if (partitionFetchState.isActive)
          requestMap.put(topicPartition, partitionFetchState.offset)
      }
      new DummyFetchRequest(requestMap)
    }

    override def handlePartitionsWithErrors(partitions: Iterable[TopicPartition]) = delayPartitions(partitions, fetchBackOffMs.toLong)

  }

}

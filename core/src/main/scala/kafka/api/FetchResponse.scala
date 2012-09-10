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

package kafka.api

import java.nio.ByteBuffer
import java.nio.channels.GatheringByteChannel
import kafka.common.ErrorMapping
import kafka.message.{MessageSet, ByteBufferMessageSet}
import kafka.network.{MultiSend, Send}
import kafka.utils.Utils

object PartitionData {
  def readFrom(buffer: ByteBuffer): PartitionData = {
    val error = buffer.getShort
    val partition = buffer.getInt
    val initialOffset = buffer.getLong
    val hw = buffer.getLong()
    val messageSetSize = buffer.getInt
    val messageSetBuffer = buffer.slice()
    messageSetBuffer.limit(messageSetSize)
    buffer.position(buffer.position + messageSetSize)
    new PartitionData(partition, error, initialOffset, hw, new ByteBufferMessageSet(messageSetBuffer, initialOffset))
  }
}

case class PartitionData(partition: Int, error: Short = ErrorMapping.NoError, initialOffset:Long = 0L, hw: Long = -1L, messages: MessageSet) {
  val sizeInBytes = 4 + 2 + 8 + 4 + messages.sizeInBytes.intValue() + 8

  def this(partition: Int, messages: MessageSet) = this(partition, ErrorMapping.NoError, 0L, -1L, messages)
}

// SENDS

class PartitionDataSend(val partitionData: PartitionData) extends Send {
  private val messageSize = partitionData.messages.sizeInBytes
  private var messagesSentSize = 0L

  private val buffer = ByteBuffer.allocate(26)
  buffer.putShort(partitionData.error)
  buffer.putInt(partitionData.partition)
  buffer.putLong(partitionData.initialOffset)
  buffer.putLong(partitionData.hw)
  buffer.putInt(partitionData.messages.sizeInBytes.intValue())
  buffer.rewind()

  def complete = !buffer.hasRemaining && messagesSentSize >= messageSize

  def writeTo(channel: GatheringByteChannel): Int = {
    var written = 0
    if(buffer.hasRemaining)
      written += channel.write(buffer)
    if(!buffer.hasRemaining && messagesSentSize < messageSize) {
      val bytesSent = partitionData.messages.writeTo(channel, messagesSentSize, messageSize - messagesSentSize).toInt
      messagesSentSize += bytesSent
      written += bytesSent
    }
    written
  }
}

object TopicData {
  def readFrom(buffer: ByteBuffer): TopicData = {
    val topic = Utils.readShortString(buffer, "UTF-8")
    val partitionCount = buffer.getInt
    val partitions = new Array[PartitionData](partitionCount)
    for(i <- 0 until partitionCount)
      partitions(i) = PartitionData.readFrom(buffer)
    new TopicData(topic, partitions.sortBy(_.partition))
  }

  def findPartition(data: Array[PartitionData], partition: Int): Option[PartitionData] = {
    if(data == null || data.size == 0)
      return None

    var (low, high) = (0, data.size-1)
    while(low <= high) {
      val mid = (low + high) / 2
      val found = data(mid)
      if(found.partition == partition)
        return Some(found)
      else if(partition < found.partition)
        high = mid - 1
      else
        low = mid + 1
    }
    None
  }
}

case class TopicData(topic: String, partitionDataArray: Array[PartitionData]) {
  val sizeInBytes = 2 + topic.length + partitionDataArray.foldLeft(4)(_ + _.sizeInBytes)

  // need to override equals due to brokern java-arrays equals functionality
  override def equals(other: Any): Boolean = {
    other match {
      case that: TopicData =>
        ( topic == that.topic &&
          partitionDataArray.toSeq == that.partitionDataArray.toSeq )
      case _ => false
    }
  }
}

class TopicDataSend(val topicData: TopicData) extends Send {
  val size = topicData.sizeInBytes

  var sent = 0

  private val buffer = ByteBuffer.allocate(2 + topicData.topic.length() + 4)
  Utils.writeShortString(buffer, topicData.topic, "UTF-8")
  buffer.putInt(topicData.partitionDataArray.length)
  buffer.rewind()

  val sends = new MultiSend(topicData.partitionDataArray.map(new PartitionDataSend(_)).toList) {
    val expectedBytesToWrite = topicData.partitionDataArray.foldLeft(0)(_ + _.sizeInBytes)
  }

  def complete = sent >= size

  def writeTo(channel: GatheringByteChannel): Int = {
    expectIncomplete()
    var written = 0
    if(buffer.hasRemaining)
      written += channel.write(buffer)
    if(!buffer.hasRemaining && !sends.complete) {
      written += sends.writeCompletely(channel)
    }
    sent += written
    written
  }
}




object FetchResponse {
  def readFrom(buffer: ByteBuffer): FetchResponse = {
    val versionId = buffer.getShort
    val errorCode = buffer.getShort
    val correlationId = buffer.getInt
    val dataCount = buffer.getInt
    val data = new Array[TopicData](dataCount)
    for(i <- 0 until data.length)
      data(i) = TopicData.readFrom(buffer)
    new FetchResponse(versionId, correlationId, data, errorCode)
  }
}


case class FetchResponse(versionId: Short,
                         correlationId: Int,
                         data: Array[TopicData],
                         errorCode: Short = ErrorMapping.NoError)  {

  val sizeInBytes = 2 + 4 + 2 + data.foldLeft(4)(_ + _.sizeInBytes)

  lazy val topicMap = data.groupBy(_.topic).mapValues(_.head)

  def messageSet(topic: String, partition: Int): ByteBufferMessageSet = {
    val messageSet = topicMap.get(topic) match {
      case Some(topicData) =>
        TopicData.findPartition(topicData.partitionDataArray, partition).map(_.messages).getOrElse(MessageSet.Empty)
      case None =>
        MessageSet.Empty
    }
    messageSet.asInstanceOf[ByteBufferMessageSet]
  }

  def highWatermark(topic: String, partition: Int): Long = {
    topicMap.get(topic) match {
      case Some(topicData) =>
        TopicData.findPartition(topicData.partitionDataArray, partition).map(_.hw).getOrElse(-1L)
      case None => -1L
    }
  }
}


class FetchResponseSend(val fetchResponse: FetchResponse) extends Send {
  private val size = fetchResponse.sizeInBytes
  private var sent = 0
  
  private val buffer = ByteBuffer.allocate(16)
  buffer.putInt(size)
  buffer.putShort(fetchResponse.versionId)
  buffer.putShort(fetchResponse.errorCode)
  buffer.putInt(fetchResponse.correlationId)
  buffer.putInt(fetchResponse.data.length)
  buffer.rewind()
  
  val sends = new MultiSend(fetchResponse.data.map(new TopicDataSend(_)).toList) {
    val expectedBytesToWrite = fetchResponse.data.foldLeft(0)(_ + _.sizeInBytes)
  }

  def complete = sent >= sendSize

  def writeTo(channel: GatheringByteChannel):Int = {
    expectIncomplete()
    var written = 0
    if(buffer.hasRemaining)
      written += channel.write(buffer)
    if(!buffer.hasRemaining && !sends.complete) {
      written += sends.writeCompletely(channel)
    }
    sent += written
    written
  }

  def sendSize = 4 + fetchResponse.sizeInBytes
}

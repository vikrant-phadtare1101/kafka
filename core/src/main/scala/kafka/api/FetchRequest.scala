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

import java.nio._
import kafka.network._
import kafka.utils._
import scala.collection.mutable.{HashMap, Buffer, ListBuffer}

object OffsetDetail {

  def readFrom(buffer: ByteBuffer): OffsetDetail = {
    val topic = Utils.readShortString(buffer, "UTF-8")

    val partitionsCount = buffer.getInt
    val partitions = new Array[Int](partitionsCount)
    for (i <- 0 until partitions.length)
      partitions(i) = buffer.getInt

    val offsetsCount = buffer.getInt
    val offsets = new Array[Long](offsetsCount)
    for (i <- 0 until offsets.length)
      offsets(i) = buffer.getLong

    val fetchesCount = buffer.getInt
    val fetchSizes = new Array[Int](fetchesCount)
    for (i <- 0 until fetchSizes.length)
      fetchSizes(i) = buffer.getInt

    new OffsetDetail(topic, partitions, offsets, fetchSizes)
  }

}

case class OffsetDetail(topic: String, partitions: Seq[Int], offsets: Seq[Long], fetchSizes: Seq[Int]) {

  def writeTo(buffer: ByteBuffer) {
    Utils.writeShortString(buffer, topic, "UTF-8")

    if(partitions.size > Int.MaxValue || offsets.size > Int.MaxValue || fetchSizes.size > Int.MaxValue)
      throw new IllegalArgumentException("Number of fetches in FetchRequest exceeds " + Int.MaxValue + ".")

    buffer.putInt(partitions.length)
    partitions.foreach(buffer.putInt(_))

    buffer.putInt(offsets.length)
    offsets.foreach(buffer.putLong(_))

    buffer.putInt(fetchSizes.length)
    fetchSizes.foreach(buffer.putInt(_))
  }

  def sizeInBytes(): Int = {
    2 + topic.length() +                              // topic string
      partitions.foldLeft(4)((s, _) => s + 4) +       // each request partition (int)
      offsets.foldLeft(4)((s, _) => s + 8) +          // each request offset (long)
      fetchSizes.foldLeft(4)((s,_) => s + 4)          // each request fetch size
  }
}

object FetchRequest {
  val CurrentVersion = 1.shortValue()

  def readFrom(buffer: ByteBuffer): FetchRequest = {
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = Utils.readShortString(buffer, "UTF-8")
    val replicaId = buffer.getInt
    val maxWait = buffer.getInt
    val minBytes = buffer.getInt
    val offsetsCount = buffer.getInt
    val offsetInfo = new Array[OffsetDetail](offsetsCount)
    for(i <- 0 until offsetInfo.length)
      offsetInfo(i) = OffsetDetail.readFrom(buffer)

    new FetchRequest(versionId, correlationId, clientId, replicaId, maxWait, minBytes, offsetInfo)
  }

}

case class FetchRequest( versionId: Short,
                         correlationId: Int,
                         clientId: String,
                         replicaId: Int,
                         maxWait: Int,
                         minBytes: Int,
                         offsetInfo: Seq[OffsetDetail] ) extends Request(RequestKeys.Fetch) {

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    Utils.writeShortString(buffer, clientId, "UTF-8")
    buffer.putInt(replicaId)
    buffer.putInt(maxWait)
    buffer.putInt(minBytes)
    buffer.putInt(offsetInfo.size)
    for(topicDetail <- offsetInfo) {
      topicDetail.writeTo(buffer)
    }
  }

  def sizeInBytes: Int = 2 + 4 + (2 + clientId.length()) + 4 + 4 + 4 + offsetInfo.foldLeft(4)(_ + _.sizeInBytes())
}

class FetchRequestBuilder() {
  private var correlationId = -1
  private val versionId = FetchRequest.CurrentVersion
  private var clientId = ""
  private var replicaId = -1        // sensible default
  private var maxWait = -1          // sensible default
  private var minBytes = -1         // sensible default
  private val requestMap = new HashMap[String, Tuple3[Buffer[Int], Buffer[Long], Buffer[Int]]]

  def addFetch(topic: String, partition: Int, offset: Long, fetchSize: Int) = {
    val topicData = requestMap.getOrElseUpdate(topic, (ListBuffer[Int](), ListBuffer[Long](), ListBuffer[Int]()))
    topicData._1.append(partition)
    topicData._2.append(offset)
    topicData._3.append(fetchSize)
    this
  }

  def correlationId(correlationId: Int): FetchRequestBuilder = {
    this.correlationId = correlationId
    this
  }

  def clientId(clientId: String): FetchRequestBuilder = {
    this.clientId = clientId
    this
  }

  def replicaId(replicaId: Int): FetchRequestBuilder = {
    this.replicaId = replicaId
    this
  }

  def maxWait(maxWait: Int): FetchRequestBuilder = {
    this.maxWait = maxWait
    this
  }

  def minBytes(minBytes: Int): FetchRequestBuilder = {
    this.minBytes = minBytes
    this
  }

  def build() = {
    val offsetDetails = requestMap.map{ topicData =>
      new OffsetDetail(topicData._1, topicData._2._1.toArray, topicData._2._2.toArray, topicData._2._3.toArray)
    }
    new FetchRequest(versionId, correlationId, clientId, replicaId, maxWait, minBytes, offsetDetails.toArray[OffsetDetail])
  }
}

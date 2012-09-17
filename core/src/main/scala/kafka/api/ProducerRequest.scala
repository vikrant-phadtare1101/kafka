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
import kafka.message._
import kafka.utils._
import scala.collection.Map
import kafka.common.TopicAndPartition


object ProducerRequest {
  val CurrentVersion: Short = 0

  def readFrom(buffer: ByteBuffer): ProducerRequest = {
    val versionId: Short = buffer.getShort
    val correlationId: Int = buffer.getInt
    val clientId: String = Utils.readShortString(buffer, RequestOrResponse.DefaultCharset)
    val requiredAcks: Short = buffer.getShort
    val ackTimeoutMs: Int = buffer.getInt
    //build the topic structure
    val topicCount = buffer.getInt
    val partitionDataPairs = (1 to topicCount).flatMap(_ => {
      // process topic
      val topic = Utils.readShortString(buffer, RequestOrResponse.DefaultCharset)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partition = buffer.getInt
        val messageSetSize = buffer.getInt
        val messageSetBuffer = new Array[Byte](messageSetSize)
        buffer.get(messageSetBuffer,0,messageSetSize)
        (TopicAndPartition(topic, partition),
         new PartitionData(partition,new ByteBufferMessageSet(ByteBuffer.wrap(messageSetBuffer))))
      })
    })

    ProducerRequest(versionId, correlationId, clientId, requiredAcks, ackTimeoutMs, Map(partitionDataPairs:_*))
  }
}

case class ProducerRequest( versionId: Short = ProducerRequest.CurrentVersion,
                            correlationId: Int,
                            clientId: String,
                            requiredAcks: Short,
                            ackTimeoutMs: Int,
                            data: Map[TopicAndPartition, PartitionData])
    extends RequestOrResponse(Some(RequestKeys.ProduceKey)) {

  /**
   * Partitions the data into a map of maps (one for each topic).
   */
  private lazy val dataGroupedByTopic = data.groupBy(_._1.topic)

  def this(correlationId: Int,
           clientId: String,
           requiredAcks: Short,
           ackTimeoutMs: Int,
           data: Map[TopicAndPartition, PartitionData]) =
    this(ProducerRequest.CurrentVersion, correlationId, clientId, requiredAcks, ackTimeoutMs, data)

  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    Utils.writeShortString(buffer, clientId, RequestOrResponse.DefaultCharset)
    buffer.putShort(requiredAcks)
    buffer.putInt(ackTimeoutMs)

    //save the topic structure
    buffer.putInt(dataGroupedByTopic.size) //the number of topics
    dataGroupedByTopic.foreach {
      case (topic, topicAndPartitionData) =>
        Utils.writeShortString(buffer, topic, RequestOrResponse.DefaultCharset) //write the topic
        buffer.putInt(topicAndPartitionData.size) //the number of partitions
        topicAndPartitionData.foreach(partitionAndData => {
          val partitionData = partitionAndData._2
          buffer.putInt(partitionData.partition)
          buffer.putInt(partitionData.messages.asInstanceOf[ByteBufferMessageSet].buffer.limit)
          buffer.put(partitionData.messages.asInstanceOf[ByteBufferMessageSet].buffer)
          partitionData.messages.asInstanceOf[ByteBufferMessageSet].buffer.rewind
        })
    }
  }

  def sizeInBytes: Int = {
    2 + /* versionId */
    4 + /* correlationId */
    Utils.shortStringLength(clientId, RequestOrResponse.DefaultCharset) + /* client id */
    2 + /* requiredAcks */
    4 + /* ackTimeoutMs */
    4 + /* number of topics */
    dataGroupedByTopic.foldLeft(0)((foldedTopics, currTopic) => {
      foldedTopics +
      Utils.shortStringLength(currTopic._1, RequestOrResponse.DefaultCharset) +
      4 + /* the number of partitions */
      {
        currTopic._2.foldLeft(0)((foldedPartitions, currPartition) => {
          foldedPartitions +
          4 + /* partition id */
          4 + /* byte-length of serialized messages */
          currPartition._2.messages.sizeInBytes.toInt
        })
      }
    })
  }

  def numPartitions = data.size

}


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
import kafka.cluster.Broker
import kafka.common.ErrorMapping

object ConsumerMetadataResponse {
  val CurrentVersion = 0
  
  def readFrom(buffer: ByteBuffer) = {
    val correlationId = buffer.getInt
    val errorCode = buffer.getShort
    val coordinatorOpt = if (errorCode == ErrorMapping.NoError)
      Some(Broker.readFrom(buffer))
    else
      None

    ConsumerMetadataResponse(coordinatorOpt, errorCode, correlationId)
  }
  
}

case class ConsumerMetadataResponse (coordinator: Option[Broker], errorCode: Short, override val correlationId: Int = 0)
  extends RequestOrResponse(correlationId = correlationId) {

  def sizeInBytes =
    4 + /* correlationId */
    2 + /* error code */
    coordinator.map(_.sizeInBytes).getOrElse(0)

  def writeTo(buffer: ByteBuffer) {
    buffer.putInt(correlationId)
    buffer.putShort(errorCode)
    if (errorCode == ErrorMapping.NoError) {
      coordinator.get.writeTo(buffer)
    }
  }

  def describe(details: Boolean) = toString
}
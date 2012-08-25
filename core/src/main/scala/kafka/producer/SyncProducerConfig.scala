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
import kafka.utils.VerifiableProperties

class SyncProducerConfig private (val props: VerifiableProperties) extends SyncProducerConfigShared {
  def this(originalProps: Properties) {
    this(new VerifiableProperties(originalProps))
    // no need to verify the property since SyncProducerConfig is supposed to be used internally
  }

  /** the broker to which the producer sends events */
  val host = props.getString("host")

  /** the port on which the broker is running */
  val port = props.getInt("port")
}

trait SyncProducerConfigShared {
  val props: VerifiableProperties
  
  val bufferSize = props.getInt("buffer.size", 100*1024)

  val connectTimeoutMs = props.getInt("connect.timeout.ms", 5000)

  val reconnectInterval = props.getInt("reconnect.interval", 30000)

  /** negative reconnect time interval means disabling this time-based reconnect feature */
  var reconnectTimeInterval = props.getInt("reconnect.time.interval.ms", 1000*1000*10)

  val maxMessageSize = props.getInt("max.message.size", 1000000)

  /* the client application sending the producer requests */
  val correlationId = props.getInt("producer.request.correlation_id", SyncProducerConfig.DefaultCorrelationId)

  /* the client application sending the producer requests */
  val clientId = props.getString("producer.request.client_id",SyncProducerConfig.DefaultClientId)

  /*
   * The required acks of the producer requests - negative value means ack
   * after the replicas in ISR have caught up to the leader's offset
   * corresponding to this produce request.
   */
  val requiredAcks = props.getShort("producer.request.required.acks", SyncProducerConfig.DefaultRequiredAcks)

  /*
   * The ack timeout of the producer requests. Value must be non-negative and non-zero
   */
  val requestTimeoutMs = props.getIntInRange("producer.request.timeout.ms", SyncProducerConfig.DefaultAckTimeoutMs,
                                             (1, Integer.MAX_VALUE))
}

object SyncProducerConfig {
  val DefaultCorrelationId = -1
  val DefaultClientId = ""
  val DefaultRequiredAcks : Short = 0
  val DefaultAckTimeoutMs = 500
}
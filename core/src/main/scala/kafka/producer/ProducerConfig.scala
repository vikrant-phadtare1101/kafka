/*
 * Copyright 2010 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import async.AsyncProducerConfigShared
import java.util.Properties
import kafka.utils.{ZKConfig, Utils}
import kafka.common.InvalidConfigException

class ProducerConfig(val props: Properties) extends ZKConfig(props) 
        with AsyncProducerConfigShared with SyncProducerConfigShared{

  /** For bypassing zookeeper based auto partition discovery, use this config   *
   *  to pass in static broker and per-broker partition information. Format-    *
   *  brokerid1:host1:port1, brokerid2:host2:port2*/
  val brokerPartitionInfo = Utils.getString(props, "broker.list", null)
  if(brokerPartitionInfo != null && Utils.getString(props, "partitioner.class", null) != null)
    throw new InvalidConfigException("partitioner.class cannot be used when broker.list is set")

  /** the partitioner class for partitioning events amongst sub-topics */
  val partitionerClass = Utils.getString(props, "partitioner.class", "kafka.producer.DefaultPartitioner")

  /** this parameter specifies whether the messages are sent asynchronously *
   * or not. Valid values are - async for asynchronous send                 *
   *                            sync for synchronous send                   */
  val producerType = Utils.getString(props, "producer.type", "sync")

  /**
   * This parameter allows you to specify the compression codec for all data generated *
   * by this producer. The default is NoCompressionCodec
   */
  val compressionCodec = Utils.getCompressionCodec(props, "compression.codec")

  /** This parameter allows you to set whether compression should be turned *
   *  on for particular topics
   *
   *  If the compression codec is anything other than NoCompressionCodec,
   *
   *    Enable compression only for specified topics if any
   *
   *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
   *
   *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
   */
  val compressedTopics = Utils.getCSVList(Utils.getString(props, "compressed.topics", null))
}

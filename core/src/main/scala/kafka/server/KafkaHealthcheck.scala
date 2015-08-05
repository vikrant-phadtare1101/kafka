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

import kafka.cluster.EndPoint
import kafka.utils._
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.I0Itec.zkclient.{IZkStateListener, ZkClient}
import java.net.InetAddress


/**
 * This class registers the broker in zookeeper to allow 
 * other brokers and consumers to detect failures. It uses an ephemeral znode with the path:
 *   /brokers/[0...N] --> advertisedHost:advertisedPort
 *   
 * Right now our definition of health is fairly naive. If we register in zk we are healthy, otherwise
 * we are dead.
 */
class KafkaHealthcheck(private val brokerId: Int,
                       private val advertisedEndpoints: Map[SecurityProtocol, EndPoint],
                       private val zkSessionTimeoutMs: Int,
                       private val zkClient: ZkClient) extends Logging {

  val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + brokerId
  val sessionExpireListener = new SessionExpireListener

  def startup() {
    zkClient.subscribeStateChanges(sessionExpireListener)
    register()
  }

  /**
   * Register this broker as "alive" in zookeeper
   */
  def register() {
    val jmxPort = System.getProperty("com.sun.management.jmxremote.port", "-1").toInt
    val updatedEndpoints = advertisedEndpoints.mapValues(endpoint =>
      if (endpoint.host == null || endpoint.host.trim.isEmpty)
        EndPoint(InetAddress.getLocalHost.getCanonicalHostName, endpoint.port, endpoint.protocolType)
      else
        endpoint
    )

    // the default host and port are here for compatibility with older client
    // only PLAINTEXT is supported as default
    // if the broker doesn't listen on PLAINTEXT protocol, an empty endpoint will be registered and older clients will be unable to connect
    val plaintextEndpoint = updatedEndpoints.getOrElse(SecurityProtocol.PLAINTEXT, new EndPoint(null,-1,null))
    ZkUtils.registerBrokerInZk(zkClient, brokerId, plaintextEndpoint.host, plaintextEndpoint.port, updatedEndpoints, zkSessionTimeoutMs, jmxPort)
  }

  /**
   *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
   *  connection for us. We need to re-register this broker in the broker registry.
   */
  class SessionExpireListener() extends IZkStateListener {
    @throws(classOf[Exception])
    def handleStateChanged(state: KeeperState) {
      // do nothing, since zkclient will do reconnect for us.
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
     *
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleNewSession() {
      info("re-registering broker info in ZK for broker " + brokerId)
      register()
      info("done re-registering broker")
      info("Subscribing to %s path to watch for new topics".format(ZkUtils.BrokerTopicsPath))
    }

    override def handleSessionEstablishmentError(error: Throwable): Unit = {
      fatal("Could not establish session with zookeeper", error)
    }
  }

}

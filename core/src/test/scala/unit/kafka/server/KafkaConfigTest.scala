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
import kafka.api.{ApiVersion, KAFKA_082}
import kafka.utils.{TestUtils, CoreUtils}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.Test
import org.scalatest.junit.JUnit3Suite

class KafkaConfigTest extends JUnit3Suite {

  @Test
  def testLogRetentionTimeHoursProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LogRetentionTimeHoursProp, "1")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(60L * 60L * 1000L, cfg.logRetentionTimeMillis)
  }
  
  @Test
  def testLogRetentionTimeMinutesProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LogRetentionTimeMinutesProp, "30")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }
  
  @Test
  def testLogRetentionTimeMsProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LogRetentionTimeMillisProp, "1800000")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }
  
  @Test
  def testLogRetentionTimeNoConfigProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(24 * 7 * 60L * 60L * 1000L, cfg.logRetentionTimeMillis)
  }
  
  @Test
  def testLogRetentionTimeBothMinutesAndHoursProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LogRetentionTimeMinutesProp, "30")
    props.put(KafkaConfig.LogRetentionTimeHoursProp, "1")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }
  
  @Test
  def testLogRetentionTimeBothMinutesAndMsProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LogRetentionTimeMillisProp, "1800000")
    props.put(KafkaConfig.LogRetentionTimeMinutesProp, "10")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }
  @Test
  def testLogRetentionUnlimited() {
    val props1 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)
    val props2 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)
    val props3 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)
    val props4 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)
    val props5 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)

    props1.put("log.retention.ms", "-1")
    props2.put("log.retention.minutes", "-1")
    props3.put("log.retention.hours", "-1")

    val cfg1 = KafkaConfig.fromProps(props1)
    val cfg2 = KafkaConfig.fromProps(props2)
    val cfg3 = KafkaConfig.fromProps(props3)
    assertEquals("Should be -1", -1, cfg1.logRetentionTimeMillis)
    assertEquals("Should be -1", -1, cfg2.logRetentionTimeMillis)
    assertEquals("Should be -1", -1, cfg3.logRetentionTimeMillis)

    props4.put("log.retention.ms", "-1")
    props4.put("log.retention.minutes", "30")

    val cfg4 = KafkaConfig.fromProps(props4)
    assertEquals("Should be -1", -1, cfg4.logRetentionTimeMillis)

    props5.put("log.retention.ms", "0")

    intercept[IllegalArgumentException] {
      val cfg5 = KafkaConfig.fromProps(props5)
    }
  }

  @Test
  def testLogRetentionValid {
    val props1 = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val props2 = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val props3 = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)

    props1.put("log.retention.ms", "0")
    props2.put("log.retention.minutes", "0")
    props3.put("log.retention.hours", "0")

    intercept[IllegalArgumentException] {
      val cfg1 = KafkaConfig.fromProps(props1)
    }
    intercept[IllegalArgumentException] {
      val cfg2 = KafkaConfig.fromProps(props2)
    }
    intercept[IllegalArgumentException] {
      val cfg3 = KafkaConfig.fromProps(props3)
    }

  }

  @Test
  def testAdvertiseDefaults() {
    val port = "9999"
    val hostName = "fake-host"

    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.remove(KafkaConfig.ListenersProp)
    props.put(KafkaConfig.HostNameProp, hostName)
    props.put(KafkaConfig.PortProp, port)
    val serverConfig = KafkaConfig.fromProps(props)
    val endpoints = serverConfig.advertisedListeners
    val endpoint = endpoints.get(SecurityProtocol.PLAINTEXT).get
    assertEquals(endpoint.host, hostName)
    assertEquals(endpoint.port, port.toInt)
  }

  @Test
  def testAdvertiseConfigured() {
    val advertisedHostName = "routable-host"
    val advertisedPort = "1234"

    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put(KafkaConfig.AdvertisedHostNameProp, advertisedHostName)
    props.put(KafkaConfig.AdvertisedPortProp, advertisedPort)

    val serverConfig = KafkaConfig.fromProps(props)
    val endpoints = serverConfig.advertisedListeners
    val endpoint = endpoints.get(SecurityProtocol.PLAINTEXT).get
    
    assertEquals(endpoint.host, advertisedHostName)
    assertEquals(endpoint.port, advertisedPort.toInt)
  }
    
  @Test
  def testAdvertisePortDefault() {
    val advertisedHostName = "routable-host"
    val port = "9999"

    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put(KafkaConfig.AdvertisedHostNameProp, advertisedHostName)
    props.put(KafkaConfig.PortProp, port)

    val serverConfig = KafkaConfig.fromProps(props)
    val endpoints = serverConfig.advertisedListeners
    val endpoint = endpoints.get(SecurityProtocol.PLAINTEXT).get

    assertEquals(endpoint.host, advertisedHostName)
    assertEquals(endpoint.port, port.toInt)
  }
  
  @Test
  def testAdvertiseHostNameDefault() {
    val hostName = "routable-host"
    val advertisedPort = "9999"

    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put(KafkaConfig.HostNameProp, hostName)
    props.put(KafkaConfig.AdvertisedPortProp, advertisedPort)

    val serverConfig = KafkaConfig.fromProps(props)
    val endpoints = serverConfig.advertisedListeners
    val endpoint = endpoints.get(SecurityProtocol.PLAINTEXT).get

    assertEquals(endpoint.host, hostName)
    assertEquals(endpoint.port, advertisedPort.toInt)
  }    
    
  @Test
  def testDuplicateListeners() {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, "1")
    props.put(KafkaConfig.ZkConnectProp, "localhost:2181")

    // listeners with duplicate port
    props.put(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9091,TRACE://localhost:9091")
    assert(!isValidKafkaConfig(props))

    // listeners with duplicate protocol
    props.put(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9091,PLAINTEXT://localhost:9092")
    assert(!isValidKafkaConfig(props))

    // advertised listeners with duplicate port
    props.put(KafkaConfig.AdvertisedListenersProp, "PLAINTEXT://localhost:9091,TRACE://localhost:9091")
    assert(!isValidKafkaConfig(props))
  }

  @Test
  def testBadListenerProtocol() {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, "1")
    props.put(KafkaConfig.ZkConnectProp, "localhost:2181")
    props.put(KafkaConfig.ListenersProp, "BAD://localhost:9091")

    assert(!isValidKafkaConfig(props))
  }

  @Test
  def testListenerDefaults() {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, "1")
    props.put(KafkaConfig.ZkConnectProp, "localhost:2181")

    // configuration with host and port, but no listeners
    props.put(KafkaConfig.HostNameProp, "myhost")
    props.put(KafkaConfig.PortProp, "1111")

    val conf = KafkaConfig.fromProps(props)
    assertEquals(CoreUtils.listenerListToEndPoints("PLAINTEXT://myhost:1111"), conf.listeners)

    // configuration with null host
    props.remove(KafkaConfig.HostNameProp)

    val conf2 = KafkaConfig.fromProps(props)
    assertEquals(CoreUtils.listenerListToEndPoints("PLAINTEXT://:1111"), conf2.listeners)
    assertEquals(CoreUtils.listenerListToEndPoints("PLAINTEXT://:1111"), conf2.advertisedListeners)
    assertEquals(null, conf2.listeners(SecurityProtocol.PLAINTEXT).host)

    // configuration with advertised host and port, and no advertised listeners
    props.put(KafkaConfig.AdvertisedHostNameProp, "otherhost")
    props.put(KafkaConfig.AdvertisedPortProp, "2222")

    val conf3 = KafkaConfig.fromProps(props)
    assertEquals(conf3.advertisedListeners, CoreUtils.listenerListToEndPoints("PLAINTEXT://otherhost:2222"))
  }

  @Test
  def testVersionConfiguration() {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, "1")
    props.put(KafkaConfig.ZkConnectProp, "localhost:2181")
    val conf = KafkaConfig.fromProps(props)
    assertEquals(ApiVersion.latestVersion, conf.interBrokerProtocolVersion)

    props.put(KafkaConfig.InterBrokerProtocolVersionProp,"0.8.2.0")
    val conf2 = KafkaConfig.fromProps(props)
    assertEquals(KAFKA_082, conf2.interBrokerProtocolVersion)

    // check that 0.8.2.0 is the same as 0.8.2.1
    props.put(KafkaConfig.InterBrokerProtocolVersionProp,"0.8.2.1")
    val conf3 = KafkaConfig.fromProps(props)
    assertEquals(KAFKA_082, conf3.interBrokerProtocolVersion)

    //check that latest is newer than 0.8.2
    assert(ApiVersion.latestVersion.onOrAfter(conf3.interBrokerProtocolVersion))
  }

  private def isValidKafkaConfig(props: Properties): Boolean = {
    try {
      KafkaConfig.fromProps(props)
      true
    } catch {
      case e: IllegalArgumentException => false
    }
  }

  @Test
  def testUncleanLeaderElectionDefault() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, true)
  }

  @Test
  def testUncleanElectionDisabled() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.UncleanLeaderElectionEnableProp, String.valueOf(false))
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, false)
  }

  @Test
  def testUncleanElectionEnabled() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.UncleanLeaderElectionEnableProp, String.valueOf(true))
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, true)
  }

  @Test
  def testUncleanElectionInvalid() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.UncleanLeaderElectionEnableProp, "invalid")

    intercept[ConfigException] {
      KafkaConfig.fromProps(props)
    }
  }
  
  @Test
  def testLogRollTimeMsProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LogRollTimeMillisProp, "1800000")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(30 * 60L * 1000L, cfg.logRollTimeMillis)
  }
  
  @Test
  def testLogRollTimeBothMsAndHoursProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LogRollTimeMillisProp, "1800000")
    props.put(KafkaConfig.LogRollTimeHoursProp, "1")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRollTimeMillis)
  }
    
  @Test
  def testLogRollTimeNoConfigProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(24 * 7 * 60L * 60L * 1000L, cfg.logRollTimeMillis																									)
  }

  @Test
  def testDefaultCompressionType() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.compressionType, "producer")
  }

  @Test
  def testValidCompressionType() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put("compression.type", "gzip")
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.compressionType, "gzip")
  }

  @Test
  def testInvalidCompressionType() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.CompressionTypeProp, "abc")
    intercept[IllegalArgumentException] {
      KafkaConfig.fromProps(props)
    }
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.util.concurrent.{ConcurrentHashMap, DelayQueue, TimeUnit}
import kafka.utils.{ShutdownableThread, Logging}
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.{Total, Rate, Avg}
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.kafka.common.utils.Time

/**
 * Represents the sensors aggregated per client
 * @param quotaSensor @Sensor that tracks the quota
 * @param throttleTimeSensor @Sensor that tracks the throttle time
 */
private case class ClientSensors(quotaSensor: Sensor, throttleTimeSensor: Sensor)

/**
 * Configuration settings for quota management
 * @param quotaBytesPerSecondDefault The default bytes per second quota allocated to any client
 * @param numQuotaSamples The number of samples to retain in memory
 * @param quotaWindowSizeSeconds The time span of each sample
 *
 */
case class ClientQuotaManagerConfig(quotaBytesPerSecondDefault: Long =
                                        ClientQuotaManagerConfig.QuotaBytesPerSecondDefault,
                                    numQuotaSamples: Int =
                                        ClientQuotaManagerConfig.DefaultNumQuotaSamples,
                                    quotaWindowSizeSeconds: Int =
                                        ClientQuotaManagerConfig.DefaultQuotaWindowSizeSeconds)

object ClientQuotaManagerConfig {
  val QuotaBytesPerSecondDefault = Long.MaxValue
  // Always have 10 whole windows + 1 current window
  val DefaultNumQuotaSamples = 11
  val DefaultQuotaWindowSizeSeconds = 1
  // Purge sensors after 1 hour of inactivity
  val InactiveSensorExpirationTimeSeconds  = 3600
}

/**
 * Helper class that records per-client metrics. It is also responsible for maintaining Quota usage statistics
 * for all clients.
 * @param config @ClientQuotaManagerConfig quota configs
 * @param metrics @Metrics Metrics instance
 * @param apiKey API Key for the request
 * @param time @Time object to use
 */
class ClientQuotaManager(private val config: ClientQuotaManagerConfig,
                         private val metrics: Metrics,
                         private val apiKey: QuotaType,
                         private val time: Time) extends Logging {
  private val overriddenQuota = new ConcurrentHashMap[String, Quota]()
  private val defaultQuota = Quota.upperBound(config.quotaBytesPerSecondDefault)
  private val lock = new ReentrantReadWriteLock()
  private val delayQueue = new DelayQueue[ThrottledResponse]()
  private val sensorAccessor = new SensorAccess
  val throttledRequestReaper = new ThrottledRequestReaper(delayQueue)
  throttledRequestReaper.start()

  private val delayQueueSensor = metrics.sensor(apiKey + "-delayQueue")
  delayQueueSensor.add(metrics.metricName("queue-size",
                                      apiKey.toString,
                                      "Tracks the size of the delay queue"), new Total())

  /**
   * Reaper thread that triggers callbacks on all throttled requests
   * @param delayQueue DelayQueue to dequeue from
   */
  class ThrottledRequestReaper(delayQueue: DelayQueue[ThrottledResponse]) extends ShutdownableThread(
    "ThrottledRequestReaper-%s".format(apiKey), false) {

    override def doWork(): Unit = {
      val response: ThrottledResponse = delayQueue.poll(1, TimeUnit.SECONDS)
      if (response != null) {
        // Decrement the size of the delay queue
        delayQueueSensor.record(-1)
        trace("Response throttled for: " + response.throttleTimeMs + " ms")
        response.execute()
      }
    }
  }

  /**
   * Records that a clientId changed some metric being throttled (produced/consumed bytes, QPS etc.)
   * @param clientId clientId that produced the data
   * @param value amount of data written in bytes
   * @param callback Callback function. This will be triggered immediately if quota is not violated.
   *                 If there is a quota violation, this callback will be triggered after a delay
   * @return Number of milliseconds to delay the response in case of Quota violation.
   *         Zero otherwise
   */
  def recordAndMaybeThrottle(clientId: String, value: Int, callback: Int => Unit): Int = {
    val clientSensors = getOrCreateQuotaSensors(clientId)
    var throttleTimeMs = 0
    try {
      clientSensors.quotaSensor.record(value)
      // trigger the callback immediately if quota is not violated
      callback(0)
    } catch {
      case qve: QuotaViolationException =>
        // Compute the delay
        val clientMetric = metrics.metrics().get(clientRateMetricName(clientId))
        throttleTimeMs = throttleTime(clientMetric, getQuotaMetricConfig(quota(clientId)))
        clientSensors.throttleTimeSensor.record(throttleTimeMs)
        // If delayed, add the element to the delayQueue
        delayQueue.add(new ThrottledResponse(time, throttleTimeMs, callback))
        delayQueueSensor.record()
        logger.debug("Quota violated for sensor (%s). Delay time: (%d)".format(clientSensors.quotaSensor.name(), throttleTimeMs))
    }
    throttleTimeMs
  }

  /*
   * This calculates the amount of time needed to bring the metric within quota
   * assuming that no new metrics are recorded.
   *
   * Basically, if O is the observed rate and T is the target rate over a window of W, to bring O down to T,
   * we need to add a delay of X to W such that O * W / (W + X) = T.
   * Solving for X, we get X = (O - T)/T * W.
   */
  private def throttleTime(clientMetric: KafkaMetric, config: MetricConfig): Int = {
    val rateMetric: Rate = measurableAsRate(clientMetric.metricName(), clientMetric.measurable())
    val quota = config.quota()
    val difference = clientMetric.value() - quota.bound
    // Use the precise window used by the rate calculation
    val throttleTimeMs = difference / quota.bound * rateMetric.windowSize(config, time.milliseconds())
    throttleTimeMs.round.toInt
  }

  // Casting to Rate because we only use Rate in Quota computation
  private def measurableAsRate(name: MetricName, measurable: Measurable): Rate = {
    measurable match {
      case r: Rate => r
      case _ => throw new IllegalArgumentException(s"Metric $name is not a Rate metric, value $measurable")
    }
  }

  /**
   * Returns the quota for the specified clientId
   */
  def quota(clientId: String): Quota =
    if (overriddenQuota.containsKey(clientId)) overriddenQuota.get(clientId) else defaultQuota

  /*
   * This function either returns the sensors for a given client id or creates them if they don't exist
   * First sensor of the tuple is the quota enforcement sensor. Second one is the throttle time sensor
   */
  private def getOrCreateQuotaSensors(clientId: String): ClientSensors = {
    ClientSensors(
      sensorAccessor.getOrCreate(
        getQuotaSensorName(clientId),
        ClientQuotaManagerConfig.InactiveSensorExpirationTimeSeconds,
        lock, metrics,
        () => clientRateMetricName(clientId),
        () => getQuotaMetricConfig(quota(clientId)),
        () => new Rate()
      ),
      sensorAccessor.getOrCreate(getThrottleTimeSensorName(clientId),
        ClientQuotaManagerConfig.InactiveSensorExpirationTimeSeconds,
        lock,
        metrics,
        () => metrics.metricName("throttle-time", apiKey.toString, "Tracking average throttle-time per client", "client-id", clientId),
        () => null,
        () => new Avg()
      )
    )
  }

  private def getThrottleTimeSensorName(clientId: String): String = apiKey + "ThrottleTime-" + clientId

  private def getQuotaSensorName(clientId: String): String = apiKey + "-" + clientId

  private def getQuotaMetricConfig(quota: Quota): MetricConfig = {
    new MetricConfig()
            .timeWindow(config.quotaWindowSizeSeconds, TimeUnit.SECONDS)
            .samples(config.numQuotaSamples)
            .quota(quota)
  }

  /**
   * Reset quotas to the default value for the given clientId
   * @param clientId client to override
   */
  def resetQuota(clientId: String) = {
    updateQuota(clientId, defaultQuota)
  }

  /**
   * Overrides quotas per clientId
   * @param clientId client to override
   * @param quota custom quota to apply
   */
  def updateQuota(clientId: String, quota: Quota) = {
    /*
     * Acquire the write lock to apply changes in the quota objects.
     * This method changes the quota in the overriddenQuota map and applies the update on the actual KafkaMetric object (if it exists).
     * If the KafkaMetric hasn't been created, the most recent value will be used from the overriddenQuota map.
     * The write lock prevents quota update and creation at the same time. It also guards against concurrent quota change
     * notifications
     */
    lock.writeLock().lock()
    try {
      logger.info(s"Changing quota for clientId $clientId to ${quota.bound()}")

      if (quota.equals(defaultQuota))
        this.overriddenQuota.remove(clientId)
      else
        this.overriddenQuota.put(clientId, quota)

      // Change the underlying metric config if the sensor has been created.
      // Note the metric could be expired by another thread, so use a local variable and null check.
      val metric = metrics.metrics.get(clientRateMetricName(clientId))
      if (metric != null) {
        logger.info(s"Sensor for clientId $clientId already exists. Changing quota to ${quota.bound()} in MetricConfig")
        metric.config(getQuotaMetricConfig(quota))
      }
    } finally {
      lock.writeLock().unlock()
    }
  }

  private def clientRateMetricName(clientId: String): MetricName = {
    metrics.metricName("byte-rate", apiKey.toString,
                   "Tracking byte-rate per client",
                   "client-id", clientId)
  }

  def shutdown() = {
    throttledRequestReaper.shutdown()
  }
}

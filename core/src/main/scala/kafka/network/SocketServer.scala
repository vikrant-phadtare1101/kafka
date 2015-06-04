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

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels._
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.EndPoint
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.InvalidReceiveException
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.{SystemTime, Time, Utils}

import scala.collection._

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val brokerId: Int,
                   val endpoints: Map[SecurityProtocol, EndPoint],
                   val numProcessorThreads: Int,
                   val maxQueuedRequests: Int,
                   val sendBufferSize: Int,
                   val recvBufferSize: Int,
                   val maxRequestSize: Int = Int.MaxValue,
                   val maxConnectionsPerIp: Int = Int.MaxValue,
                   val connectionsMaxIdleMs: Long,
                   val maxConnectionsPerIpOverrides: Map[String, Int],
                   val time: Time,
                   val metrics: Metrics) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Socket Server on Broker " + brokerId + "], "

  private val processors = new Array[Processor](numProcessorThreads)
  private[network] var acceptors =  mutable.Map[EndPoint,Acceptor]()
  val requestChannel = new RequestChannel(numProcessorThreads, maxQueuedRequests)

  private val allMetricNames = (0 until numProcessorThreads).map { i =>
    val tags = new util.HashMap[String, String]()
    tags.put("networkProcessor", i.toString)
    new MetricName("io-wait-ratio", "socket-server-metrics", tags)
  }

  /* I'm pushing the mapping of port-to-protocol to the processor level,
     so the processor can put the correct protocol in the request channel.
     we'll probably have a more elegant way of doing this once we patch the request channel
     to include more information about security and authentication.
     TODO: re-consider this code when working on KAFKA-1683
   */
  private val portToProtocol: ConcurrentHashMap[Int, SecurityProtocol] = new ConcurrentHashMap[Int, SecurityProtocol]()

  /**
   * Start the socket server
   */
  def startup() {
    val quotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {
        def value = allMetricNames.map( metricName =>
          metrics.metrics().get(metricName).value()).sum / numProcessorThreads
      }
    )


    this.synchronized {
      for (i <- 0 until numProcessorThreads) {
        processors(i) = new Processor(i,
          time,
          maxRequestSize,
          numProcessorThreads,
          requestChannel,
          quotas,
          connectionsMaxIdleMs,
          portToProtocol,
          metrics
          )
        Utils.newThread("kafka-network-thread-%d-%d".format(brokerId, i), processors(i), false).start()
      }
    }

    // register the processor threads for notification of responses
    requestChannel.addResponseListener((id:Int) => processors(id).wakeup())
   
    // start accepting connections
    // right now we will use the same processors for all ports, since we didn't implement different protocols
    // in the future, we may implement different processors for SSL and Kerberos

    this.synchronized {
      endpoints.values.foreach(endpoint => {
        val acceptor = new Acceptor(endpoint.host, endpoint.port, processors, sendBufferSize, recvBufferSize, quotas, endpoint.protocolType, portToProtocol)
        acceptors.put(endpoint, acceptor)
        Utils.newThread("kafka-socket-acceptor-%s-%d".format(endpoint.protocolType.toString, endpoint.port), acceptor, false).start()
        acceptor.awaitStartup
      })
    }

    info("Started " + acceptors.size + " acceptor threads")
  }

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    this.synchronized {
      acceptors.values.foreach(_.shutdown)
      processors.foreach(_.shutdown)
    }
    info("Shutdown completed")
  }

  def boundPort(protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int = {
    try {
      acceptors(endpoints(protocol)).serverChannel.socket().getLocalPort
    } catch {
      case e: Exception => throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }
}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  private val startupLatch = new CountDownLatch(1)
  private val shutdownLatch = new CountDownLatch(1)
  private val alive = new AtomicBoolean(true)

  def wakeup()

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false)
    wakeup()
    shutdownLatch.await()
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete() = {
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete() = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning = alive.get
  
  /**
   * Close the given key and associated socket
   */
  def close(key: SelectionKey) {
    if(key != null) {
      key.attach(null)
      close(key.channel.asInstanceOf[SocketChannel])
      swallowError(key.cancel())
    }
  }
  
  def close(channel: SocketChannel) {
    if(channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      swallowError(channel.socket().close())
      swallowError(channel.close())
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is only need for one of these
 */
private[kafka] class Acceptor(val host: String, 
                              private val port: Int,
                              private val processors: Array[Processor],
                              val sendBufferSize: Int, 
                              val recvBufferSize: Int,
                              connectionQuotas: ConnectionQuotas,
                              protocol: SecurityProtocol,
                              portToProtocol: ConcurrentHashMap[Int, SecurityProtocol]) extends AbstractServerThread(connectionQuotas) {
  val nioSelector = java.nio.channels.Selector.open()
  val serverChannel = openServerSocket(host, port)
  portToProtocol.put(serverChannel.socket().getLocalPort, protocol)

  /**
   * Accept loop that checks for new connection attempts
   */
  def run() {
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT);
    startupComplete()
    var currentProcessor = 0
    while(isRunning) {
      val ready = nioSelector.select(500)
      if(ready > 0) {
        val keys = nioSelector.selectedKeys()
        val iter = keys.iterator()
        while(iter.hasNext && isRunning) {
          var key: SelectionKey = null
          try {
            key = iter.next
            iter.remove()
            if(key.isAcceptable)
              accept(key, processors(currentProcessor))
            else
               throw new IllegalStateException("Unrecognized key state for acceptor thread.")

            // round robin to the next processor thread
            currentProcessor = (currentProcessor + 1) % processors.length
          } catch {
            case e: Throwable => error("Error while accepting connection", e)
          }
        }
      }
    }
    debug("Closing server socket and selector.")
    swallowError(serverChannel.close())
    swallowError(nioSelector.close())
    shutdownComplete()
  }
  
  /*
   * Create a server socket to listen for connections on.
   */
  def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress = 
      if(host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    serverChannel.socket().setReceiveBufferSize(recvBufferSize)
    try {
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostName, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException => 
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostName, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   */
  def accept(key: SelectionKey, processor: Processor) {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    try {
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)
      socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s. sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getInetAddress, socketChannel.socket.getLocalSocketAddress,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))

      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selectors
 */
private[kafka] class Processor(val id: Int,
                               val time: Time,
                               val maxRequestSize: Int,
                               val totalProcessorThreads: Int,
                               val requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               val connectionsMaxIdleMs: Long,
                               val portToProtocol: ConcurrentHashMap[Int,SecurityProtocol],
                               val metrics: Metrics) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()

  private val metricTags = new util.HashMap[String, String]()
  metricTags.put("networkProcessor", id.toString)

  newGauge("IdlePercent",
    new Gauge[Double] {
      def value = {
        metrics.metrics().get(new MetricName("io-wait-ratio", "socket-server-metrics", metricTags)).value()
      }
    },
    JavaConversions.mapAsScalaMap(metricTags)
  )

  private val selector = new org.apache.kafka.common.network.Selector(
    maxRequestSize,
    connectionsMaxIdleMs,
    metrics,
    time,
    "socket-server",
    metricTags,
    false)

  override def run() {
    startupComplete()
    while(isRunning) {
      // setup any new connections that have been queued up
      configureNewConnections()
      // register any new responses for writing
      processNewResponses()

      try {
        selector.poll(300)
      } catch {
        case e @ (_: IllegalStateException | _: IOException) => {
          error("Closing processor %s due to illegal state or IO exception".format(id))
          swallow(closeAll())
          shutdownComplete()
          throw e
        }
        case e: InvalidReceiveException =>
          // Log warning and continue since Selector already closed the connection
          warn("Connection was closed due to invalid receive. Processor will continue handling other connections")
      }
      collection.JavaConversions.collectionAsScalaIterable(selector.completedReceives).foreach( receive => {
        try {
          val req = RequestChannel.Request(processor = id, connectionId = receive.source, buffer = receive.payload, startTimeMs = time.milliseconds, securityProtocol = SecurityProtocol.PLAINTEXT)
          requestChannel.sendRequest(req)
        } catch {
          case e @ (_: InvalidRequestException | _: SchemaException) => {
            // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
            error("Closing socket for " + receive.source + " because of error", e)
            selector.close(receive.source)
          }
        }
        selector.mute(receive.source)
      })

      collection.JavaConversions.iterableAsScalaIterable(selector.completedSends()).foreach( send => {
        val resp = inflightResponses.remove(send.destination()).get
        resp.request.updateRequestMetrics()
        selector.unmute(send.destination())
      })
    }



    debug("Closing selector - processor " + id)
    closeAll()
    shutdownComplete()
  }

  private def processNewResponses() {
    var curr = requestChannel.receiveResponse(id)
    while(curr != null) {
      try {
        curr.responseAction match {
          case RequestChannel.NoOpAction => {
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            curr.request.updateRequestMetrics
            trace("Socket server received empty response to send, registering for read: " + curr)
            selector.unmute(curr.request.connectionId)
          }
          case RequestChannel.SendAction => {
            trace("Socket server received response to send, registering for write and sending data: " + curr)
            selector.send(curr.responseSend)
            inflightResponses += (curr.request.connectionId -> curr)
          }
          case RequestChannel.CloseConnectionAction => {
            curr.request.updateRequestMetrics
            trace("Closing socket connection actively according to the response code.")
            selector.close(curr.request.connectionId)
          }
        }


      } finally {
        curr = requestChannel.receiveResponse(id)
      }
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  private def configureNewConnections() {
    while(!newConnections.isEmpty) {
      val channel = newConnections.poll()
      debug("Processor " + id + " listening to new connection from " + channel.socket.getRemoteSocketAddress)
      val localHost = channel.socket().getLocalAddress.getHostAddress
      val localPort = channel.socket().getLocalPort
      val remoteHost = channel.socket().getInetAddress.getHostAddress
      val remotePort = channel.socket().getPort
      val connectionId = localHost + ":" + localPort + "-" + remoteHost + ":" + remotePort
      selector.register(connectionId, channel)
    }
  }

  /**
   * Close all open connections
   */
  def closeAll() {
    selector.close()
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = selector.wakeup()

}

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {
  private val overrides = overrideQuotas.map(entry => (InetAddress.getByName(entry._1), entry._2))
  private val counts = mutable.Map[InetAddress, Int]()
  
  def inc(addr: InetAddress) {
    counts synchronized {
      val count = counts.getOrElse(addr, 0)
      counts.put(addr, count + 1)
      val max = overrides.getOrElse(addr, defaultMax)
      if(count >= max)
        throw new TooManyConnectionsException(addr, max)
    }
  }
  
  def dec(addr: InetAddress) {
    counts synchronized {
      val count = counts.get(addr).get
      if(count == 1)
        counts.remove(addr)
      else
        counts.put(addr, count - 1)
    }
  }
  
}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))

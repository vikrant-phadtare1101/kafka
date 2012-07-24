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

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.net._
import java.io._
import java.nio.channels._

import kafka.utils._

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val port: Int,
                   val numProcessorThreads: Int, 
                   val monitoringPeriodSecs: Int,
                   val maxQueuedRequests: Int,
                   val maxRequestSize: Int = Int.MaxValue) extends Logging {

  private val time = SystemTime
  private val processors = new Array[Processor](numProcessorThreads)
  private var acceptor: Acceptor = new Acceptor(port, processors)
  val stats: SocketServerStats = new SocketServerStats(1000L * 1000L * 1000L * monitoringPeriodSecs)
  val requestChannel = new RequestChannel(numProcessorThreads, maxQueuedRequests)

  /**
   * Start the socket server
   */
  def startup() {
    for(i <- 0 until numProcessorThreads) {
      processors(i) = new Processor(i, time, maxRequestSize, requestChannel, stats)
      Utils.newThread("kafka-processor-%d-%d".format(port, i), processors(i), false).start()
    }
    // register the processor threads for notification of responses
    requestChannel.addResponseListener((id:Int) => processors(id).wakeup())
   
    // start accepting connections
    Utils.newThread("kafka-acceptor", acceptor, false).start()
    acceptor.awaitStartup
    info("Kafka socket server started")
  }

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down socket server")
    acceptor.shutdown
    for(processor <- processors)
      processor.shutdown
    info("Shut down socket server complete")
  }
}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread extends Runnable with Logging {

  protected val selector = Selector.open();
  private val startupLatch = new CountDownLatch(1)
  private val shutdownLatch = new CountDownLatch(1)
  private val alive = new AtomicBoolean(false)

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false)
    selector.wakeup()
    shutdownLatch.await
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete() = {
    alive.set(true)
    startupLatch.countDown
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete() = shutdownLatch.countDown

  /**
   * Is the server still running?
   */
  protected def isRunning = alive.get
  
  /**
   * Wakeup the thread for selection.
   */
  def wakeup() = selector.wakeup()
  
}

/**
 * Thread that accepts and configures new connections. There is only need for one of these
 */
private[kafka] class Acceptor(val port: Int, private val processors: Array[Processor]) extends AbstractServerThread {

  /**
   * Accept loop that checks for new connection attempts
   */
  def run() {
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    serverChannel.socket.bind(new InetSocketAddress(port))
    serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    info("Awaiting connections on port " + port)
    startupComplete()
    var currentProcessor = 0
    while(isRunning) {
      val ready = selector.select(500)
      if(ready > 0) {
        val keys = selector.selectedKeys()
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
            case e: Throwable => error("Error in acceptor", e)
          }
        }
      }
    }
    debug("Closing server socket and selector.")
    swallowError(serverChannel.close())
    swallowError(selector.close())
    shutdownComplete()
  }

  /*
   * Accept a new connection
   */
  def accept(key: SelectionKey, processor: Processor) {
    val socketChannel = key.channel().asInstanceOf[ServerSocketChannel].accept()
    debug("Accepted connection from " + socketChannel.socket.getInetAddress() + " on " + socketChannel.socket.getLocalSocketAddress)
    socketChannel.configureBlocking(false)
    socketChannel.socket().setTcpNoDelay(true)
    processor.accept(socketChannel)
  }

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selectors
 */
private[kafka] class Processor(val id: Int,
                               val time: Time, 
                               val maxRequestSize: Int,
                               val requestChannel: RequestChannel,
                               val stats: SocketServerStats) extends AbstractServerThread {
  
  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]();

  override def run() {
    startupComplete()
    while(isRunning) {
      // setup any new connections that have been queued up
      configureNewConnections()
      // register any new responses for writing
      processNewResponses()
      val ready = selector.select(300)
      if(ready > 0) {
        val keys = selector.selectedKeys()
        val iter = keys.iterator()
        while(iter.hasNext && isRunning) {
          var key: SelectionKey = null
          try {
            key = iter.next
            iter.remove()
            if(key.isReadable)
              read(key)
            else if(key.isWritable)
              write(key)
            else if(!key.isValid)
              close(key)
            else
              throw new IllegalStateException("Unrecognized key state for processor thread.")
          } catch {
            case e: EOFException => {
              info("Closing socket connection to %s.".format(channelFor(key).socket.getInetAddress))
              close(key)
            } case e: InvalidRequestException => {
              info("Closing socket connection to %s due to invalid request: %s".format(channelFor(key).socket.getInetAddress, e.getMessage))
              close(key)
            } case e: Throwable => {
              error("Closing socket for " + channelFor(key).socket.getInetAddress + " because of error", e)
              close(key)
            }
          }
        }
      }
    }
    debug("Closing selector.")
    swallowError(selector.close())
    shutdownComplete()
  }

  private def processNewResponses() {
    var curr = requestChannel.receiveResponse(id)
    while(curr != null) {
      trace("Socket server received response to send, registering for write: " + curr)
      val key = curr.requestKey.asInstanceOf[SelectionKey]
      try {
        key.interestOps(SelectionKey.OP_WRITE)
        key.attach(curr.response)
      } catch {
        case e: CancelledKeyException => {
          debug("Ignoring response for closed socket.")
          close(key)
        }
      }finally {
        curr = requestChannel.receiveResponse(id)
      }
    }
  }
  
  private def close(key: SelectionKey) {
    val channel = key.channel.asInstanceOf[SocketChannel]
    debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
    swallowError(channel.socket().close())
    swallowError(channel.close())
    key.attach(null)
    swallowError(key.cancel())
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
    while(newConnections.size() > 0) {
      val channel = newConnections.poll()
      debug("Listening to new connection from " + channel.socket.getRemoteSocketAddress)
      channel.register(selector, SelectionKey.OP_READ)
    }
  }

  /*
   * Process reads from ready sockets
   */
  def read(key: SelectionKey) {
    val socketChannel = channelFor(key)
    var request = key.attachment.asInstanceOf[Receive]
    if(key.attachment == null) {
      request = new BoundedByteBufferReceive(maxRequestSize)
      key.attach(request)
    }
    val read = request.readFrom(socketChannel)
    stats.recordBytesRead(read)
    trace(read + " bytes read from " + socketChannel.socket.getRemoteSocketAddress())
    if(read < 0) {
      close(key)
    } else if(request.complete) {
      val req = RequestChannel.Request(processor = id, requestKey = key, request = request, start = time.nanoseconds)
      requestChannel.sendRequest(req)
      trace("Recieved request, sending for processing by handler: " + req)
      key.attach(null)
    } else {
      // more reading to be done
      key.interestOps(SelectionKey.OP_READ)
      wakeup()
    }
  }

  /*
   * Process writes to ready sockets
   */
  def write(key: SelectionKey) {
    val socketChannel = channelFor(key)
    var response = key.attachment().asInstanceOf[Send]
    if(response == null)
      throw new IllegalStateException("Registered for write interest but no response attached to key.")
    val written = response.writeTo(socketChannel)
    stats.recordBytesWritten(written)
    trace(written + " bytes written to " + socketChannel.socket.getRemoteSocketAddress())
    if(response.complete) {
      key.attach(null)
      key.interestOps(SelectionKey.OP_READ)
    } else {
      key.interestOps(SelectionKey.OP_WRITE)
      wakeup()
    }
  }

  private def channelFor(key: SelectionKey) = key.channel().asInstanceOf[SocketChannel]

}

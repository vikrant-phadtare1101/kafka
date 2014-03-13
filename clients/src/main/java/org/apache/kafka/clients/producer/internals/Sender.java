/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The background thread that handles the sending of produce requests to the Kafka cluster. This thread makes metadata
 * requests to renew its view of the cluster and then sends produce requests to the appropriate nodes.
 */
public class Sender implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Sender.class);

    /* the state of each nodes connection */
    private final NodeStates nodeStates;

    /* the record accumulator that batches records */
    private final RecordAccumulator accumulator;

    /* the selector used to perform network i/o */
    private final Selectable selector;

    /* the client id used to identify this client in requests to the server */
    private final String clientId;

    /* the maximum request size to attempt to send to the server */
    private final int maxRequestSize;

    /* the number of acknowledgements to request from the server */
    private final short acks;

    /* the max time in ms for the server to wait for acknowlegements */
    private final int requestTimeout;

    /* the number of times to retry a failed request before giving up */
    private final int retries;

    /* the socket send buffer size in bytes */
    private final int socketSendBuffer;

    /* the socket receive size buffer in bytes */
    private final int socketReceiveBuffer;

    /* the set of currently in-flight requests awaiting a response from the server */
    private final InFlightRequests inFlightRequests;

    /* a reference to the current Cluster instance */
    private final Metadata metadata;

    /* the clock instance used for getting the time */
    private final Time time;

    /* the current node to attempt to use for metadata requests (will round-robin over nodes) */
    private int metadataFetchNodeIndex;

    /* the current correlation id to use when sending requests to servers */
    private int correlation;

    /* true iff there is a metadata request that has been sent and for which we have not yet received a response */
    private boolean metadataFetchInProgress;

    /* true while the sender thread is still running */
    private volatile boolean running;

    public Sender(Selectable selector,
                  Metadata metadata,
                  RecordAccumulator accumulator,
                  String clientId,
                  int maxRequestSize,
                  long reconnectBackoffMs,
                  short acks,
                  int retries,
                  int requestTimeout,
                  int socketSendBuffer,
                  int socketReceiveBuffer,
                  Time time) {
        this.nodeStates = new NodeStates(reconnectBackoffMs);
        this.accumulator = accumulator;
        this.selector = selector;
        this.maxRequestSize = maxRequestSize;
        this.metadata = metadata;
        this.clientId = clientId;
        this.running = true;
        this.requestTimeout = requestTimeout;
        this.acks = acks;
        this.retries = retries;
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.inFlightRequests = new InFlightRequests();
        this.correlation = 0;
        this.metadataFetchInProgress = false;
        this.time = time;
        this.metadataFetchNodeIndex = new Random().nextInt();
    }

    /**
     * The main run loop for the sender thread
     */
    public void run() {
        log.debug("Starting Kafka producer I/O thread.");

        // main loop, runs until close is called
        while (running) {
            try {
                run(time.milliseconds());
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

        // okay we stopped accepting requests but there may still be
        // requests in the accumulator or waiting for acknowledgment,
        // wait until these are completed.
        do {
            try {
                run(time.milliseconds());
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        } while (this.accumulator.hasUnsent() || this.inFlightRequests.totalInFlightRequests() > 0);

        // close all the connections
        this.selector.close();

        log.debug("Shutdown of Kafka producer I/O thread has completed.");
    }

    /**
     * Run a single iteration of sending
     * 
     * @param now The current time
     */
    public void run(long now) {
        Cluster cluster = metadata.fetch();
        // get the list of partitions with data ready to send
        List<TopicPartition> ready = this.accumulator.ready(now);

        // should we update our metadata?
        List<NetworkSend> sends = new ArrayList<NetworkSend>();
        maybeUpdateMetadata(cluster, sends, now);

        // prune the list of ready topics to eliminate any that we aren't ready to send yet
        List<TopicPartition> sendable = processReadyPartitions(cluster, ready, now);

        // create produce requests
        List<RecordBatch> batches = this.accumulator.drain(sendable, this.maxRequestSize);
        List<InFlightRequest> requests = collate(cluster, batches);

        if (ready.size() > 0) {
            log.trace("Partitions with complete batches: {}", ready);
            log.trace("Partitions ready to initiate a request: {}", sendable);
            log.trace("Created {} produce requests: {}", requests.size(), requests);
        }

        for (int i = 0; i < requests.size(); i++) {
            InFlightRequest request = requests.get(i);
            this.inFlightRequests.add(request);
            sends.add(request.request);
        }

        // do the I/O
        try {
            this.selector.poll(100L, sends);
        } catch (IOException e) {
            log.error("Unexpected error during I/O in producer network thread", e);
        }

        // handle responses, connections, and disconnections
        handleSends(this.selector.completedSends());
        handleResponses(this.selector.completedReceives(), time.milliseconds());
        handleDisconnects(this.selector.disconnected(), time.milliseconds());
        handleConnects(this.selector.connected());
    }

    /**
     * Add a metadata request to the list of sends if we need to make one
     */
    private void maybeUpdateMetadata(Cluster cluster, List<NetworkSend> sends, long now) {
        if (this.metadataFetchInProgress || !metadata.needsUpdate(now))
            return;

        Node node = selectMetadataDestination(cluster);
        if (node == null)
            return;

        if (nodeStates.isConnected(node.id())) {
            Set<String> topics = metadata.topics();
            this.metadataFetchInProgress = true;
            InFlightRequest metadataRequest = metadataRequest(node.id(), topics);
            log.debug("Sending metadata request {} to node {}", metadataRequest, node.id());
            sends.add(metadataRequest.request);
            this.inFlightRequests.add(metadataRequest);
        } else if (nodeStates.canConnect(node.id(), now)) {
            // we don't have a connection to this node right now, make one
            initiateConnect(node, now);
        }
    }

    /**
     * Find a good node to make a metadata request to. This method will first look for a node that has an existing
     * connection and no outstanding requests. If there are no such nodes it will look for a node with no outstanding
     * requests.
     * @return A node with no requests currently being sent or null if no such node exists
     */
    private Node selectMetadataDestination(Cluster cluster) {
        List<Node> nodes = cluster.nodes();

        // first look for a node to which we are connected and have no outstanding requests
        boolean connectionInProgress = false;
        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(metadataNodeIndex(i, nodes.size()));
            if (nodeStates.isConnected(node.id()) && this.inFlightRequests.canSendMore(node.id())) {
                this.metadataFetchNodeIndex = metadataNodeIndex(i + 1, nodes.size());
                return node;
            } else if (nodeStates.isConnecting(node.id())) {
                connectionInProgress = true;
            }
        }

        // if we have a connection that is being established now, just wait for that don't make another
        if (connectionInProgress)
            return null;

        // okay, no luck, pick a random unused node
        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(metadataNodeIndex(i, nodes.size()));
            if (this.inFlightRequests.canSendMore(node.id())) {
                this.metadataFetchNodeIndex = metadataNodeIndex(i + 1, nodes.size());
                return node;
            }
        }

        return null; // we failed to find a good destination
    }

    /**
     * Get the index in the node list of the node to use for the metadata request
     */
    private int metadataNodeIndex(int offset, int size) {
        return Utils.abs(offset + this.metadataFetchNodeIndex) % size;
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     */
    public void initiateClose() {
        this.running = false;
        this.accumulator.close();
        this.wakeup();
    }

    /**
     * Process the set of topic-partitions with data ready to send. If we have a connection to the appropriate node, add
     * it to the returned set. For any partitions we have no connection to either make one, fetch the appropriate
     * metadata to be able to do so
     */
    private List<TopicPartition> processReadyPartitions(Cluster cluster, List<TopicPartition> ready, long now) {
        List<TopicPartition> sendable = new ArrayList<TopicPartition>(ready.size());
        for (TopicPartition tp : ready) {
            Node node = cluster.leaderFor(tp);
            if (node == null) {
                // we don't know about this topic/partition or it has no leader, re-fetch metadata
                metadata.forceUpdate();
            } else if (nodeStates.isConnected(node.id()) && inFlightRequests.canSendMore(node.id())) {
                sendable.add(tp);
            } else if (nodeStates.canConnect(node.id(), now)) {
                // we don't have a connection to this node right now, make one
                initiateConnect(node, now);
            }
        }
        return sendable;
    }

    /**
     * Initiate a connection to the given node
     */
    private void initiateConnect(Node node, long now) {
        try {
            log.debug("Initiating connection to node {} at {}:{}.", node.id(), node.host(), node.port());
            selector.connect(node.id(), new InetSocketAddress(node.host(), node.port()), this.socketSendBuffer, this.socketReceiveBuffer);
            this.nodeStates.connecting(node.id(), now);
        } catch (IOException e) {
            /* attempt failed, we'll try again after the backoff */
            nodeStates.disconnected(node.id());
            /* maybe the problem is our metadata, update it */
            metadata.forceUpdate();
            log.debug("Error connecting to node {} at {}:{}:", node.id(), node.host(), node.port(), e);
        }
    }

    /**
     * Handle any closed connections
     */
    private void handleDisconnects(List<Integer> disconnects, long now) {
        // clear out the in-flight requests for the disconnected broker
        for (int node : disconnects) {
            nodeStates.disconnected(node);
            log.debug("Node {} disconnected.", node);
            for (InFlightRequest request : this.inFlightRequests.clearAll(node)) {
                log.trace("Cancelled request {} due to node {} being disconnected", request, node);
                ApiKeys requestKey = ApiKeys.forId(request.request.header().apiKey());
                switch (requestKey) {
                    case PRODUCE:
                        for (RecordBatch batch : request.batches.values()) {
                            if (canRetry(batch, Errors.NETWORK_EXCEPTION)) {
                                log.warn("Destination node disconnected for topic-partition {}, retrying ({} attempts left).",
                                    batch.topicPartition, this.retries - batch.attempts - 1);
                                this.accumulator.reenqueue(batch, now);
                            } else {
                                batch.done(-1L, new NetworkException("The server disconnected unexpectedly without sending a response."));
                                this.accumulator.deallocate(batch);
                            }
                        }
                        break;
                    case METADATA:
                        metadataFetchInProgress = false;
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected api key id: " + requestKey.id);
                }
            }
        }
        // we got a disconnect so we should probably refresh our metadata and see if that broker is dead
        if (disconnects.size() > 0)
            this.metadata.forceUpdate();
    }

    /**
     * Record any connections that completed in our node state
     */
    private void handleConnects(List<Integer> connects) {
        for (Integer id : connects) {
            log.debug("Completed connection to node {}", id);
            this.nodeStates.connected(id);
        }
    }

    /**
     * Process completed sends
     */
    public void handleSends(List<NetworkSend> sends) {
        /* if acks = 0 then the request is satisfied once sent */
        for (NetworkSend send : sends) {
            Deque<InFlightRequest> requests = this.inFlightRequests.requestQueue(send.destination());
            InFlightRequest request = requests.peekFirst();
            log.trace("Completed send of request to node {}: {}", request.request.destination(), request.request);
            if (!request.expectResponse) {
                requests.pollFirst();
                if (request.request.header().apiKey() == ApiKeys.PRODUCE.id) {
                    for (RecordBatch batch : request.batches.values()) {
                        batch.done(-1L, Errors.NONE.exception());
                        this.accumulator.deallocate(batch);
                    }
                }
            }
        }
    }

    /**
     * Handle responses from the server
     */
    private void handleResponses(List<NetworkReceive> receives, long now) {
        for (NetworkReceive receive : receives) {
            int source = receive.source();
            InFlightRequest req = inFlightRequests.nextCompleted(source);
            ResponseHeader header = ResponseHeader.parse(receive.payload());
            short apiKey = req.request.header().apiKey();
            Struct body = (Struct) ProtoUtils.currentResponseSchema(apiKey).read(receive.payload());
            correlate(req.request.header(), header);
            if (req.request.header().apiKey() == ApiKeys.PRODUCE.id) {
                log.trace("Received produce response from node {} with correlation id {}", source, req.request.header().correlationId());
                handleProduceResponse(req, req.request.header(), body, now);
            } else if (req.request.header().apiKey() == ApiKeys.METADATA.id) {
                log.trace("Received metadata response response from node {} with correlation id {}", source, req.request.header()
                    .correlationId());
                handleMetadataResponse(req.request.header(), body, now);
            } else {
                throw new IllegalStateException("Unexpected response type: " + req.request.header().apiKey());
            }
        }
    }

    private void handleMetadataResponse(RequestHeader header, Struct body, long now) {
        this.metadataFetchInProgress = false;
        MetadataResponse response = new MetadataResponse(body);
        Cluster cluster = response.cluster();
        // don't update the cluster if there are no valid nodes...the topic we want may still be in the process of being
        // created which means we will get errors and no nodes until it exists
        if (cluster.nodes().size() > 0)
            this.metadata.update(cluster, now);
        else
            log.trace("Ignoring empty metadata response with correlation id {}.", header.correlationId());
    }

    /**
     * Handle a produce response
     */
    private void handleProduceResponse(InFlightRequest request, RequestHeader header, Struct body, long now) {
        ProduceResponse pr = new ProduceResponse(body);
        for (Map<TopicPartition, ProduceResponse.PartitionResponse> responses : pr.responses().values()) {
            for (Map.Entry<TopicPartition, ProduceResponse.PartitionResponse> entry : responses.entrySet()) {
                TopicPartition tp = entry.getKey();
                ProduceResponse.PartitionResponse response = entry.getValue();
                Errors error = Errors.forCode(response.errorCode);
                if (error.exception() instanceof InvalidMetadataException)
                    metadata.forceUpdate();
                RecordBatch batch = request.batches.get(tp);
                if (canRetry(batch, error)) {
                    // retry
                    log.warn("Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                        header.correlationId(), batch.topicPartition, this.retries - batch.attempts - 1, error);
                    this.accumulator.reenqueue(batch, now);
                } else {
                    // tell the user the result of their request
                    batch.done(response.baseOffset, error.exception());
                    this.accumulator.deallocate(batch);
                }
            }
        }
    }

    /**
     * We can retry a send if the error is transient and the number of attempts taken is fewer than the maximum allowed
     */
    private boolean canRetry(RecordBatch batch, Errors error) {
        return batch.attempts < this.retries && error.exception() instanceof RetriableException;
    }

    /**
     * Validate that the response corresponds to the request we expect or else explode
     */
    private void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
        if (requestHeader.correlationId() != responseHeader.correlationId())
            throw new IllegalStateException("Correlation id for response (" + responseHeader.correlationId()
                                            + ") does not match request ("
                                            + requestHeader.correlationId()
                                            + ")");
    }

    /**
     * Create a metadata request for the given topics
     */
    private InFlightRequest metadataRequest(int node, Set<String> topics) {
        MetadataRequest metadata = new MetadataRequest(new ArrayList<String>(topics));
        RequestSend send = new RequestSend(node, header(ApiKeys.METADATA), metadata.toStruct());
        return new InFlightRequest(true, send, null);
    }

    /**
     * Collate the record batches into a list of produce requests on a per-node basis
     */
    private List<InFlightRequest> collate(Cluster cluster, List<RecordBatch> batches) {
        Map<Integer, List<RecordBatch>> collated = new HashMap<Integer, List<RecordBatch>>();
        for (RecordBatch batch : batches) {
            Node node = cluster.leaderFor(batch.topicPartition);
            List<RecordBatch> found = collated.get(node.id());
            if (found == null) {
                found = new ArrayList<RecordBatch>();
                collated.put(node.id(), found);
            }
            found.add(batch);
        }
        List<InFlightRequest> requests = new ArrayList<InFlightRequest>(collated.size());
        for (Map.Entry<Integer, List<RecordBatch>> entry : collated.entrySet())
            requests.add(produceRequest(entry.getKey(), acks, requestTimeout, entry.getValue()));
        return requests;
    }

    /**
     * Create a produce request from the given record batches
     */
    private InFlightRequest produceRequest(int destination, short acks, int timeout, List<RecordBatch> batches) {
        Map<TopicPartition, RecordBatch> batchesByPartition = new HashMap<TopicPartition, RecordBatch>();
        Map<String, List<RecordBatch>> batchesByTopic = new HashMap<String, List<RecordBatch>>();
        for (RecordBatch batch : batches) {
            batchesByPartition.put(batch.topicPartition, batch);
            List<RecordBatch> found = batchesByTopic.get(batch.topicPartition.topic());
            if (found == null) {
                found = new ArrayList<RecordBatch>();
                batchesByTopic.put(batch.topicPartition.topic(), found);
            }
            found.add(batch);
        }
        Struct produce = new Struct(ProtoUtils.currentRequestSchema(ApiKeys.PRODUCE.id));
        produce.set("acks", acks);
        produce.set("timeout", timeout);
        List<Struct> topicDatas = new ArrayList<Struct>(batchesByTopic.size());
        for (Map.Entry<String, List<RecordBatch>> entry : batchesByTopic.entrySet()) {
            Struct topicData = produce.instance("topic_data");
            topicData.set("topic", entry.getKey());
            List<RecordBatch> parts = entry.getValue();
            Object[] partitionData = new Object[parts.size()];
            for (int i = 0; i < parts.size(); i++) {
                ByteBuffer buffer = parts.get(i).records.buffer();
                buffer.flip();
                Struct part = topicData.instance("data")
                                       .set("partition", parts.get(i).topicPartition.partition())
                                       .set("record_set", buffer);
                partitionData[i] = part;
            }
            topicData.set("data", partitionData);
            topicDatas.add(topicData);
        }
        produce.set("topic_data", topicDatas.toArray());

        RequestSend send = new RequestSend(destination, header(ApiKeys.PRODUCE), produce);
        return new InFlightRequest(acks != 0, send, batchesByPartition);
    }

    private RequestHeader header(ApiKeys key) {
        return new RequestHeader(key.id, clientId, correlation++);
    }

    /**
     * Wake up the selector associated with this send thread
     */
    public void wakeup() {
        this.selector.wakeup();
    }

    /**
     * The states of a node connection
     */
    private static enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED
    }

    /**
     * The state of a node
     */
    private static final class NodeState {
        private ConnectionState state;
        private long lastConnectAttempt;

        public NodeState(ConnectionState state, long lastConnectAttempt) {
            this.state = state;
            this.lastConnectAttempt = lastConnectAttempt;
        }

        public String toString() {
            return "NodeState(" + state + ", " + lastConnectAttempt + ")";
        }
    }

    private static class NodeStates {
        private final long reconnectBackoffMs;
        private final Map<Integer, NodeState> nodeState;

        public NodeStates(long reconnectBackoffMs) {
            this.reconnectBackoffMs = reconnectBackoffMs;
            this.nodeState = new HashMap<Integer, NodeState>();
        }

        public boolean canConnect(int node, long now) {
            NodeState state = nodeState.get(node);
            if (state == null)
                return true;
            else
                return state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttempt > this.reconnectBackoffMs;
        }

        public void connecting(int node, long now) {
            nodeState.put(node, new NodeState(ConnectionState.CONNECTING, now));
        }

        public boolean isConnected(int node) {
            NodeState state = nodeState.get(node);
            return state != null && state.state == ConnectionState.CONNECTED;
        }

        public boolean isConnecting(int node) {
            NodeState state = nodeState.get(node);
            return state != null && state.state == ConnectionState.CONNECTING;
        }

        public void connected(int node) {
            nodeState(node).state = ConnectionState.CONNECTED;
        }

        public void disconnected(int node) {
            nodeState(node).state = ConnectionState.DISCONNECTED;
        }

        private NodeState nodeState(int node) {
            NodeState state = this.nodeState.get(node);
            if (state == null)
                throw new IllegalStateException("No entry found for node " + node);
            return state;
        }
    }

    /**
     * An request that hasn't been fully processed yet
     */
    private static final class InFlightRequest {
        public boolean expectResponse;
        public Map<TopicPartition, RecordBatch> batches;
        public RequestSend request;

        /**
         * @param expectResponse Should we expect a response message or is this request complete once it is sent?
         * @param request The request
         * @param batches The record batches contained in the request if it is a produce request
         */
        public InFlightRequest(boolean expectResponse, RequestSend request, Map<TopicPartition, RecordBatch> batches) {
            this.batches = batches;
            this.request = request;
            this.expectResponse = expectResponse;
        }

        @Override
        public String toString() {
            return "InFlightRequest(expectResponse=" + expectResponse + ", batches=" + batches + ", request=" + request + ")";
        }
    }

    /**
     * A set of outstanding request queues for each node that have not yet received responses
     */
    private static final class InFlightRequests {
        private final Map<Integer, Deque<InFlightRequest>> requests = new HashMap<Integer, Deque<InFlightRequest>>();

        /**
         * Add the given request to the queue for the node it was directed to
         */
        public void add(InFlightRequest request) {
            Deque<InFlightRequest> reqs = this.requests.get(request.request.destination());
            if (reqs == null) {
                reqs = new ArrayDeque<InFlightRequest>();
                this.requests.put(request.request.destination(), reqs);
            }
            reqs.addFirst(request);
        }

        public Deque<InFlightRequest> requestQueue(int node) {
            Deque<InFlightRequest> reqs = requests.get(node);
            if (reqs == null || reqs.isEmpty())
                throw new IllegalStateException("Response from server for which there are no in-flight requests.");
            return reqs;
        }

        /**
         * Get the oldest request (the one that that will be completed next) for the given node
         */
        public InFlightRequest nextCompleted(int node) {
            return requestQueue(node).pollLast();
        }

        /**
         * Can we send more requests to this node?
         * 
         * @param node Node in question
         * @return true iff we have no requests still being sent to the given node
         */
        public boolean canSendMore(int node) {
            Deque<InFlightRequest> queue = requests.get(node);
            return queue == null || queue.isEmpty() || queue.peekFirst().request.complete();
        }

        /**
         * Clear out all the in-flight requests for the given node and return them
         * 
         * @param node The node
         * @return All the in-flight requests for that node that have been removed
         */
        public Iterable<InFlightRequest> clearAll(int node) {
            Deque<InFlightRequest> reqs = requests.get(node);
            if (reqs == null) {
                return Collections.emptyList();
            } else {
                return requests.remove(node);
            }
        }

        public int totalInFlightRequests() {
            int total = 0;
            for (Deque<InFlightRequest> deque : this.requests.values())
                total += deque.size();
            return total;
        }
    }

}

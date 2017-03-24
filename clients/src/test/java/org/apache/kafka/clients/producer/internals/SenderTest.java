/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SenderTest {

    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final short ACKS_ALL = -1;
    private static final int MAX_RETRIES = 0;
    private static final String CLIENT_ID = "clientId";
    private static final String METRIC_GROUP = "producer-metrics";
    private static final double EPS = 0.0001;
    private static final int MAX_BLOCK_TIMEOUT = 1000;
    private static final int REQUEST_TIMEOUT = 1000;

    private TopicPartition tp0 = new TopicPartition("test", 0);
    private TopicPartition tp1 = new TopicPartition("test", 1);
    private MockTime time = new MockTime();
    private MockClient client = new MockClient(time);
    private int batchSize = 16 * 1024;
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE, true, new ClusterResourceListeners());
    private ApiVersions apiVersions = new ApiVersions();
    private Cluster cluster = TestUtils.singletonCluster("test", 2);
    private Metrics metrics = null;
    private RecordAccumulator accumulator = null;
    private Sender sender = null;

    @Before
    public void setup() {
        Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("client-id", CLIENT_ID);
        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        metrics = new Metrics(metricConfig, time);
        accumulator = new RecordAccumulator(batchSize, 1024 * 1024, CompressionType.NONE, 0L, 0L, metrics, time, apiVersions);
        sender = new Sender(client,
                            metadata,
                            this.accumulator,
                            true,
                            MAX_REQUEST_SIZE,
                            ACKS_ALL,
                            MAX_RETRIES,
                            metrics,
                            time,
                            REQUEST_TIMEOUT,
                            apiVersions);

        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());
    }

    @After
    public void tearDown() {
        this.metrics.close();
    }

    @Test
    public void testSimple() throws Exception {
        long offset = 0;
        Future<RecordMetadata> future = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds()); // connect
        sender.run(time.milliseconds()); // send produce request
        assertEquals("We should have a single produce request in flight.", 1, client.inFlightRequestCount());
        assertTrue(client.hasInFlightRequests());
        client.respond(produceResponse(tp0, offset, Errors.NONE, 0));
        sender.run(time.milliseconds());
        assertEquals("All requests completed.", 0, client.inFlightRequestCount());
        assertFalse(client.hasInFlightRequests());
        sender.run(time.milliseconds());
        assertTrue("Request should be completed", future.isDone());
        assertEquals(offset, future.get().offset());
    }

    @Test
    public void testMessageFormatDownConversion() throws Exception {
        // this test case verifies the behavior when the version of the produce request supported by the
        // broker changes after the record set is created

        long offset = 0;

        // start off support produce request v3
        apiVersions.update("0", NodeApiVersions.create());

        Future<RecordMetadata> future = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(),
                null, MAX_BLOCK_TIMEOUT).future;

        // now the partition leader supports only v2
        apiVersions.update("0", NodeApiVersions.create(Collections.singleton(
                new ApiVersionsResponse.ApiVersion(ApiKeys.PRODUCE.id, (short) 0, (short) 2))));

        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                ProduceRequest request = (ProduceRequest) body;
                if (request.version() != 2)
                    return false;

                MemoryRecords records = request.partitionRecordsOrFail().get(tp0);
                return records != null &&
                        records.sizeInBytes() > 0 &&
                        records.hasMatchingMagic(RecordBatch.MAGIC_VALUE_V1);
            }
        }, produceResponse(tp0, offset, Errors.NONE, 0));

        sender.run(time.milliseconds()); // connect
        sender.run(time.milliseconds()); // send produce request

        assertTrue("Request should be completed", future.isDone());
        assertEquals(offset, future.get().offset());
    }

    @Test
    public void testDownConversionForMismatchedMagicValues() throws Exception {
        // it can happen that we construct a record set with mismatching magic values (perhaps
        // because the partition leader changed after the record set was initially constructed)
        // in this case, we down-convert record sets with newer magic values to match the oldest
        // created record set

        long offset = 0;

        // start off support produce request v3
        apiVersions.update("0", NodeApiVersions.create());

        Future<RecordMetadata> future1 = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(),
                null, MAX_BLOCK_TIMEOUT).future;

        // now the partition leader supports only v2
        apiVersions.update("0", NodeApiVersions.create(Collections.singleton(
                new ApiVersionsResponse.ApiVersion(ApiKeys.PRODUCE.id, (short) 0, (short) 2))));

        Future<RecordMetadata> future2 = accumulator.append(tp1, 0L, "key".getBytes(), "value".getBytes(),
                null, MAX_BLOCK_TIMEOUT).future;

        // start off support produce request v3
        apiVersions.update("0", NodeApiVersions.create());

        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse(Errors.NONE, offset, RecordBatch.NO_TIMESTAMP);
        Map<TopicPartition, ProduceResponse.PartitionResponse> partResp = new HashMap<>();
        partResp.put(tp0, resp);
        partResp.put(tp1, resp);
        ProduceResponse produceResponse = new ProduceResponse(partResp, 0);

        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                ProduceRequest request = (ProduceRequest) body;
                if (request.version() != 2)
                    return false;

                Map<TopicPartition, MemoryRecords> recordsMap = request.partitionRecordsOrFail();
                if (recordsMap.size() != 2)
                    return false;

                for (MemoryRecords records : recordsMap.values()) {
                    if (records == null || records.sizeInBytes() == 0 || !records.hasMatchingMagic(RecordBatch.MAGIC_VALUE_V1))
                        return false;
                }
                return true;
            }
        }, produceResponse);

        sender.run(time.milliseconds()); // connect
        sender.run(time.milliseconds()); // send produce request

        assertTrue("Request should be completed", future1.isDone());
        assertTrue("Request should be completed", future2.isDone());
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    public void testQuotaMetrics() throws Exception {
        final long offset = 0;
        for (int i = 1; i <= 3; i++) {
            accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT);
            sender.run(time.milliseconds()); // send produce request
            client.respond(produceResponse(tp0, offset, Errors.NONE, 100 * i));
            sender.run(time.milliseconds());
        }
        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric avgMetric = allMetrics.get(metrics.metricName("produce-throttle-time-avg", METRIC_GROUP, ""));
        KafkaMetric maxMetric = allMetrics.get(metrics.metricName("produce-throttle-time-max", METRIC_GROUP, ""));
        assertEquals(200, avgMetric.value(), EPS);
        assertEquals(300, maxMetric.value(), EPS);
    }

    @Test
    public void testRetries() throws Exception {
        // create a sender with retries = 1
        int maxRetries = 1;
        Metrics m = new Metrics();
        try {
            Sender sender = new Sender(client,
                                       metadata,
                                       this.accumulator,
                                       false,
                                       MAX_REQUEST_SIZE,
                                       ACKS_ALL,
                                       maxRetries,
                                       m,
                                       time,
                                       REQUEST_TIMEOUT,
                                       apiVersions);
            // do a successful retry
            Future<RecordMetadata> future = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
            sender.run(time.milliseconds()); // connect
            sender.run(time.milliseconds()); // send produce request
            String id = client.requests().peek().destination();
            Node node = new Node(Integer.parseInt(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));
            client.disconnect(id);
            assertEquals(0, client.inFlightRequestCount());
            assertFalse(client.hasInFlightRequests());
            assertFalse("Client ready status should be false", client.isReady(node, 0L));
            sender.run(time.milliseconds()); // receive error
            sender.run(time.milliseconds()); // reconnect
            sender.run(time.milliseconds()); // resend
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
            long offset = 0;
            client.respond(produceResponse(tp0, offset, Errors.NONE, 0));
            sender.run(time.milliseconds());
            assertTrue("Request should have retried and completed", future.isDone());
            assertEquals(offset, future.get().offset());

            // do an unsuccessful retry
            future = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
            sender.run(time.milliseconds()); // send produce request
            for (int i = 0; i < maxRetries + 1; i++) {
                client.disconnect(client.requests().peek().destination());
                sender.run(time.milliseconds()); // receive error
                sender.run(time.milliseconds()); // reconnect
                sender.run(time.milliseconds()); // resend
            }
            sender.run(time.milliseconds());
            completedWithError(future, Errors.NETWORK_EXCEPTION);
        } finally {
            m.close();
        }
    }

    @Test
    public void testSendInOrder() throws Exception {
        int maxRetries = 1;
        Metrics m = new Metrics();
        try {
            Sender sender = new Sender(client,
                metadata,
                this.accumulator,
                true,
                MAX_REQUEST_SIZE,
                ACKS_ALL,
                maxRetries,
                m,
                time,
                REQUEST_TIMEOUT,
                apiVersions);

            // Create a two broker cluster, with partition 0 on broker 0 and partition 1 on broker 1
            Cluster cluster1 = TestUtils.clusterWith(2, "test", 2);
            metadata.update(cluster1, Collections.<String>emptySet(), time.milliseconds());

            // Send the first message.
            TopicPartition tp2 = new TopicPartition("test", 1);
            accumulator.append(tp2, 0L, "key1".getBytes(), "value1".getBytes(), null, MAX_BLOCK_TIMEOUT);
            sender.run(time.milliseconds()); // connect
            sender.run(time.milliseconds()); // send produce request
            String id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            Node node = new Node(Integer.parseInt(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));

            time.sleep(900);
            // Now send another message to tp2
            accumulator.append(tp2, 0L, "key2".getBytes(), "value2".getBytes(), null, MAX_BLOCK_TIMEOUT);

            // Update metadata before sender receives response from broker 0. Now partition 2 moves to broker 0
            Cluster cluster2 = TestUtils.singletonCluster("test", 2);
            metadata.update(cluster2, Collections.<String>emptySet(), time.milliseconds());
            // Sender should not send the second message to node 0.
            sender.run(time.milliseconds());
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
        } finally {
            m.close();
        }
    }

    /**
     * Tests that topics are added to the metadata list when messages are available to send
     * and expired if not used during a metadata refresh interval.
     */
    @Test
    public void testMetadataTopicExpiry() throws Exception {
        long offset = 0;
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time.milliseconds());

        Future<RecordMetadata> future = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertTrue("Topic not added to metadata", metadata.containsTopic(tp0.topic()));
        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());
        sender.run(time.milliseconds());  // send produce request
        client.respond(produceResponse(tp0, offset++, Errors.NONE, 0));
        sender.run(time.milliseconds());
        assertEquals("Request completed.", 0, client.inFlightRequestCount());
        assertFalse(client.hasInFlightRequests());
        sender.run(time.milliseconds());
        assertTrue("Request should be completed", future.isDone());

        assertTrue("Topic not retained in metadata list", metadata.containsTopic(tp0.topic()));
        time.sleep(Metadata.TOPIC_EXPIRY_MS);
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time.milliseconds());
        assertFalse("Unused topic has not been expired", metadata.containsTopic(tp0.topic()));
        future = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertTrue("Topic not added to metadata", metadata.containsTopic(tp0.topic()));
        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());
        sender.run(time.milliseconds());  // send produce request
        client.respond(produceResponse(tp0, offset++, Errors.NONE, 0));
        sender.run(time.milliseconds());
        assertEquals("Request completed.", 0, client.inFlightRequestCount());
        assertFalse(client.hasInFlightRequests());
        sender.run(time.milliseconds());
        assertTrue("Request should be completed", future.isDone());
    }

    private void completedWithError(Future<RecordMetadata> future, Errors error) throws Exception {
        assertTrue("Request should be completed", future.isDone());
        try {
            future.get();
            fail("Should have thrown an exception.");
        } catch (ExecutionException e) {
            assertEquals(error.exception().getClass(), e.getCause().getClass());
        }
    }

    private ProduceResponse produceResponse(TopicPartition tp, long offset, Errors error, int throttleTimeMs) {
        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse(error, offset, RecordBatch.NO_TIMESTAMP);
        Map<TopicPartition, ProduceResponse.PartitionResponse> partResp = Collections.singletonMap(tp, resp);
        return new ProduceResponse(partResp, throttleTimeMs);
    }

}

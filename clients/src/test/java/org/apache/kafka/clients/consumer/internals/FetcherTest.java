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
package org.apache.kafka.clients.consumer.internals;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class FetcherTest {

    private String topicName = "test";
    private String groupId = "test-group";
    private TopicPartition tp = new TopicPartition(topicName, 0);
    private long retryBackoffMs = 0L;
    private int minBytes = 1;
    private int maxWaitMs = 0;
    private int fetchSize = 1000;
    private String offsetReset = "EARLIEST";
    private MockTime time = new MockTime();
    private MockClient client = new MockClient(time);
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE);
    private Cluster cluster = TestUtils.singletonCluster(topicName, 1);
    private Node node = cluster.nodes().get(0);
    private SubscriptionState subscriptions = new SubscriptionState();
    private Metrics metrics = new Metrics(time);
    private Map<String, String> metricTags = new LinkedHashMap<String, String>();

    private MemoryRecords records = MemoryRecords.emptyRecords(ByteBuffer.allocate(1024), CompressionType.NONE);

    private Fetcher<byte[], byte[]> fetcher = new Fetcher<byte[], byte[]>(client,
        retryBackoffMs,
        minBytes,
        maxWaitMs,
        fetchSize,
        true, // check crc
        offsetReset,
        new ByteArrayDeserializer(),
        new ByteArrayDeserializer(),
        metadata,
        subscriptions,
        metrics,
        "consumer" + groupId,
        metricTags,
        time);

    @Before
    public void setup() throws Exception {
        metadata.update(cluster, time.milliseconds());
        client.setNode(node);

        records.append(1L, "key".getBytes(), "value-1".getBytes());
        records.append(2L, "key".getBytes(), "value-2".getBytes());
        records.append(3L, "key".getBytes(), "value-3".getBytes());
        records.close();
        records.flip();
    }

    @Test
    public void testFetchNormal() {
        List<ConsumerRecord<byte[], byte[]>> records;
        subscriptions.subscribe(tp);
        subscriptions.fetched(tp, 0);
        subscriptions.consumed(tp, 0);

        // normal fetch
        fetcher.initFetches(cluster, time.milliseconds());
        client.respond(fetchResponse(this.records.buffer(), Errors.NONE.code(), 100L));
        client.poll(0, time.milliseconds());
        records = fetcher.fetchedRecords().get(tp);
        assertEquals(3, records.size());
        assertEquals(4L, (long) subscriptions.fetched(tp)); // this is the next fetching position
        assertEquals(4L, (long) subscriptions.consumed(tp));
        long offset = 1;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            assertEquals(offset, record.offset());
            offset += 1;
        }
    }

    @Test
    public void testFetchFailed() {
        List<ConsumerRecord<byte[], byte[]>> records;
        subscriptions.subscribe(tp);
        subscriptions.fetched(tp, 0);
        subscriptions.consumed(tp, 0);

        // fetch with not leader
        fetcher.initFetches(cluster, time.milliseconds());
        client.respond(fetchResponse(this.records.buffer(), Errors.NOT_LEADER_FOR_PARTITION.code(), 100L));
        client.poll(0, time.milliseconds());
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));

        // fetch with unknown topic partition
        fetcher.initFetches(cluster, time.milliseconds());
        client.respond(fetchResponse(this.records.buffer(), Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), 100L));
        client.poll(0, time.milliseconds());
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));

        // fetch with out of range
        subscriptions.fetched(tp, 5);
        fetcher.initFetches(cluster, time.milliseconds());
        client.respond(fetchResponse(this.records.buffer(), Errors.OFFSET_OUT_OF_RANGE.code(), 100L));
        client.prepareResponse(listOffsetResponse(Collections.singletonList(0L), Errors.NONE.code()));
        client.poll(0, time.milliseconds());
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(0L, (long) subscriptions.fetched(tp));
        assertEquals(0L, (long) subscriptions.consumed(tp));
    }

    @Test
    public void testFetchOutOfRange() {
        List<ConsumerRecord<byte[], byte[]>> records;
        subscriptions.subscribe(tp);
        subscriptions.fetched(tp, 5);
        subscriptions.consumed(tp, 5);

        // fetch with out of range
        fetcher.initFetches(cluster, time.milliseconds());
        client.respond(fetchResponse(this.records.buffer(), Errors.OFFSET_OUT_OF_RANGE.code(), 100L));
        client.prepareResponse(listOffsetResponse(Collections.singletonList(0L), Errors.NONE.code()));
        client.poll(0, time.milliseconds());
        assertEquals(0, fetcher.fetchedRecords().size());
        assertEquals(0L, (long) subscriptions.fetched(tp));
        assertEquals(0L, (long) subscriptions.consumed(tp));
    }

    private Struct fetchResponse(ByteBuffer buffer, short error, long hw) {
        FetchResponse response = new FetchResponse(Collections.singletonMap(tp, new FetchResponse.PartitionData(error, hw, buffer)));
        return response.toStruct();
    }

    private Struct listOffsetResponse(List<Long> offsets, short error) {
        ListOffsetResponse response = new ListOffsetResponse(Collections.singletonMap(tp, new ListOffsetResponse.PartitionData(error, offsets)));
        return response.toStruct();
    }

}

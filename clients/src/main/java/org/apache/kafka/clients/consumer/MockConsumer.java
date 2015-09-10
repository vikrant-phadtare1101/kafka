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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A mock of the {@link Consumer} interface you can use for testing code that uses Kafka. This class is <i> not
 * threadsafe </i>
 * <p>
 * The consumer runs in the user thread and multiplexes I/O over TCP connections to each of the brokers it needs to
 * communicate with. Failure to close the consumer after use will leak these resources.
 */
public class MockConsumer<K, V> implements Consumer<K, V> {

    private final Map<String, List<PartitionInfo>> partitions;
    private final SubscriptionState subscriptions;
    private Map<TopicPartition, List<ConsumerRecord<K, V>>> records;
    private boolean closed;

    public MockConsumer(OffsetResetStrategy offsetResetStrategy) {
        this.subscriptions = new SubscriptionState(offsetResetStrategy);
        this.partitions = new HashMap<String, List<PartitionInfo>>();
        this.records = new HashMap<TopicPartition, List<ConsumerRecord<K, V>>>();
        this.closed = false;
    }
    
    @Override
    public synchronized Set<TopicPartition> assignment() {
        return this.subscriptions.assignedPartitions();
    }

    @Override
    public synchronized Set<String> subscription() {
        return this.subscriptions.subscription();
    }

    @Override
    public synchronized void subscribe(List<String> topics) {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    @Override
    public void subscribe(Pattern pattern, final ConsumerRebalanceListener listener) {
        ensureNotClosed();
        this.subscriptions.subscribe(pattern, SubscriptionState.wrapListener(this, listener));
        List<String> topicsToSubscribe = new ArrayList<>();
        for (String topic: partitions.keySet()) {
            if (pattern.matcher(topic).matches() &&
                !subscriptions.subscription().contains(topic))
                topicsToSubscribe.add(topic);
        }
        ensureNotClosed();
        this.subscriptions.changeSubscription(topicsToSubscribe);
    }

    @Override
    public synchronized void subscribe(List<String> topics, final ConsumerRebalanceListener listener) {
        ensureNotClosed();
        this.subscriptions.subscribe(topics, SubscriptionState.wrapListener(this, listener));
    }

    @Override
    public synchronized void assign(List<TopicPartition> partitions) {
        ensureNotClosed();
        this.subscriptions.assign(partitions);
    }

    @Override
    public void unsubscribe() {
        ensureNotClosed();
        subscriptions.unsubscribe();
    }

    @Override
    public synchronized ConsumerRecords<K, V> poll(long timeout) {
        ensureNotClosed();
        // update the consumed offset
        for (Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> entry : this.records.entrySet()) {
            if (!subscriptions.isPaused(entry.getKey())) {
                List<ConsumerRecord<K, V>> recs = entry.getValue();
                if (!recs.isEmpty())
                    this.subscriptions.consumed(entry.getKey(), recs.get(recs.size() - 1).offset());
            }
        }

        ConsumerRecords<K, V> copy = new ConsumerRecords<K, V>(this.records);
        this.records = new HashMap<TopicPartition, List<ConsumerRecord<K, V>>>();
        return copy;
    }

    public synchronized void addRecord(ConsumerRecord<K, V> record) {
        ensureNotClosed();
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        ArrayList<TopicPartition> currentAssigned = new ArrayList<>(this.subscriptions.assignedPartitions());
        if (!currentAssigned.contains(tp)) {
            currentAssigned.add(tp);
            this.subscriptions.changePartitionAssignment(currentAssigned);
        }
        subscriptions.seek(tp, record.offset());
        List<ConsumerRecord<K, V>> recs = this.records.get(tp);
        if (recs == null) {
            recs = new ArrayList<ConsumerRecord<K, V>>();
            this.records.put(tp, recs);
        }
        recs.add(record);
    }

    @Override
    public synchronized void commit(Map<TopicPartition, Long> offsets, CommitType commitType, ConsumerCommitCallback callback) {
        ensureNotClosed();
        for (Entry<TopicPartition, Long> entry : offsets.entrySet())
            subscriptions.committed(entry.getKey(), entry.getValue());
        if (callback != null) {
            callback.onComplete(offsets, null);
        }
    }

    @Override
    public synchronized void commit(Map<TopicPartition, Long> offsets, CommitType commitType) {
        commit(offsets, commitType, null);
    }

    @Override
    public synchronized void commit(CommitType commitType, ConsumerCommitCallback callback) {
        ensureNotClosed();
        commit(this.subscriptions.allConsumed(), commitType, callback);
    }

    @Override
    public synchronized void commit(CommitType commitType) {
        commit(commitType, null);
    }

    @Override
    public synchronized void seek(TopicPartition partition, long offset) {
        ensureNotClosed();
        subscriptions.seek(partition, offset);
    }

    @Override
    public synchronized long committed(TopicPartition partition) {
        ensureNotClosed();
        return subscriptions.committed(partition);
    }

    @Override
    public synchronized long position(TopicPartition partition) {
        ensureNotClosed();
        return subscriptions.consumed(partition);
    }

    @Override
    public synchronized void seekToBeginning(TopicPartition... partitions) {
        ensureNotClosed();
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void seekToEnd(TopicPartition... partitions) {
        ensureNotClosed();
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        ensureNotClosed();
        return Collections.emptyMap();
    }

    @Override
    public synchronized List<PartitionInfo> partitionsFor(String topic) {
        ensureNotClosed();
        List<PartitionInfo> parts = this.partitions.get(topic);
        if (parts == null)
            return Collections.emptyList();
        else
            return parts;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        ensureNotClosed();
        return partitions;
    }

    public synchronized void updatePartitions(String topic, List<PartitionInfo> partitions) {
        ensureNotClosed();
        this.partitions.put(topic, partitions);
    }

    @Override
    public void pause(TopicPartition... partitions) {
        for (TopicPartition partition : partitions)
            subscriptions.pause(partition);
    }

    @Override
    public void resume(TopicPartition... partitions) {
        for (TopicPartition partition : partitions)
            subscriptions.resume(partition);
    }

    @Override
    public synchronized void close() {
        ensureNotClosed();
        this.closed = true;
    }

    @Override
    public void wakeup() {

    }

    private void ensureNotClosed() {
        if (this.closed)
            throw new IllegalStateException("This consumer has already been closed.");
    }
}

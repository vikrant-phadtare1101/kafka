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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A mock of the {@link Consumer} interface you can use for testing code that uses Kafka. This class is <i> not
 * threadsafe </i>. However, you can use the {@link #waitForPollThen(Runnable,long)} method to write multithreaded tests
 * where a driver thread waits for {@link #poll(long)} to be called and then can safely perform operations during a
 * callback.
 */
public class MockConsumer<K, V> implements Consumer<K, V> {

    private final Map<String, List<PartitionInfo>> partitions;
    private final SubscriptionState subscriptions;
    private Map<TopicPartition, List<ConsumerRecord<K, V>>> records;
    private Set<TopicPartition> paused;
    private boolean closed;
    private final Map<TopicPartition, Long> beginningOffsets;
    private final Map<TopicPartition, Long> endOffsets;

    private AtomicReference<CountDownLatch> pollLatch;
    private KafkaException exception;

    private AtomicBoolean wakeup;

    public MockConsumer(OffsetResetStrategy offsetResetStrategy) {
        this.subscriptions = new SubscriptionState(offsetResetStrategy);
        this.partitions = new HashMap<>();
        this.records = new HashMap<>();
        this.paused = new HashSet<>();
        this.closed = false;
        this.beginningOffsets = new HashMap<>();
        this.endOffsets = new HashMap<>();
        this.pollLatch = new AtomicReference<>();
        this.exception = null;
        this.wakeup = new AtomicBoolean(false);
    }
    
    @Override
    public Set<TopicPartition> assignment() {
        return this.subscriptions.assignedPartitions();
    }

    /** Simulate a rebalance event. */
    public void rebalance(Collection<TopicPartition> newAssignment) {
        // TODO: Rebalance callbacks
        this.records.clear();
        this.subscriptions.changePartitionAssignment(newAssignment);
    }

    @Override
    public Set<String> subscription() {
        return this.subscriptions.subscription();
    }

    @Override
    public void subscribe(List<String> topics) {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    @Override
    public void subscribe(Pattern pattern, final ConsumerRebalanceListener listener) {
        ensureNotClosed();
        this.subscriptions.subscribe(pattern, listener);
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
    public void subscribe(List<String> topics, final ConsumerRebalanceListener listener) {
        ensureNotClosed();
        this.subscriptions.subscribe(topics, listener);
    }

    @Override
    public void assign(List<TopicPartition> partitions) {
        ensureNotClosed();
        this.subscriptions.assign(partitions);
    }

    @Override
    public void unsubscribe() {
        ensureNotClosed();
        subscriptions.unsubscribe();
    }

    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        ensureNotClosed();

        CountDownLatch pollLatchCopy = pollLatch.get();
        if (pollLatchCopy != null) {
            pollLatch.set(null);
            pollLatchCopy.countDown();
            synchronized (pollLatchCopy) {
                // Will block until caller of waitUntilPollThen() finishes their callback.
            }
        }

        if (wakeup.get()) {
            wakeup.set(false);
            throw new ConsumerWakeupException();
        }

        if (exception != null) {
            RuntimeException exception = this.exception;
            this.exception = null;
            throw exception;
        }

        // Handle seeks that need to wait for a poll() call to be processed
        for (TopicPartition tp : subscriptions.missingFetchPositions())
            updateFetchPosition(tp);

        // update the consumed offset
        for (Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> entry : this.records.entrySet()) {
            if (!subscriptions.isPaused(entry.getKey())) {
                List<ConsumerRecord<K, V>> recs = entry.getValue();
                if (!recs.isEmpty())
                    this.subscriptions.consumed(entry.getKey(), recs.get(recs.size() - 1).offset() + 1);
            }
        }

        ConsumerRecords<K, V> copy = new ConsumerRecords<K, V>(this.records);
        this.records = new HashMap<TopicPartition, List<ConsumerRecord<K, V>>>();
        return copy;
    }

    public void addRecord(ConsumerRecord<K, V> record) {
        ensureNotClosed();
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Set<TopicPartition> currentAssigned = new HashSet<>(this.subscriptions.assignedPartitions());
        if (!currentAssigned.contains(tp))
            throw new IllegalStateException("Cannot add records for a partition that is not assigned to the consumer");
        List<ConsumerRecord<K, V>> recs = this.records.get(tp);
        if (recs == null) {
            recs = new ArrayList<ConsumerRecord<K, V>>();
            this.records.put(tp, recs);
        }
        recs.add(record);
    }

    public void setException(KafkaException exception) {
        this.exception = exception;
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        ensureNotClosed();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet())
            subscriptions.committed(entry.getKey(), entry.getValue());
        if (callback != null) {
            callback.onComplete(offsets, null);
        }
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitAsync(offsets, null);
    }

    @Override
    public void commitAsync() {
        commitAsync(null);
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        ensureNotClosed();
        commitAsync(this.subscriptions.allConsumed(), callback);
    }

    @Override
    public void commitSync() {
        commitSync(this.subscriptions.allConsumed());
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        ensureNotClosed();
        subscriptions.seek(partition, offset);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        ensureNotClosed();
        return subscriptions.committed(partition);
    }

    @Override
    public long position(TopicPartition partition) {
        ensureNotClosed();
        if (!this.subscriptions.isAssigned(partition))
            throw new IllegalArgumentException("You can only check the position for partitions assigned to this consumer.");
        Long offset = this.subscriptions.consumed(partition);
        if (offset == null) {
            updateFetchPosition(partition);
            offset = this.subscriptions.consumed(partition);
        }
        return offset;
    }

    @Override
    public void seekToBeginning(TopicPartition... partitions) {
        ensureNotClosed();
        for (TopicPartition tp : partitions)
            subscriptions.needOffsetReset(tp, OffsetResetStrategy.EARLIEST);
    }

    public void updateBeginningOffsets(Map<TopicPartition, Long> newOffsets) {
        beginningOffsets.putAll(newOffsets);
    }

    @Override
    public void seekToEnd(TopicPartition... partitions) {
        ensureNotClosed();
        for (TopicPartition tp : partitions)
            subscriptions.needOffsetReset(tp, OffsetResetStrategy.LATEST);
    }

    public void updateEndOffsets(Map<TopicPartition, Long> newOffsets) {
        endOffsets.putAll(newOffsets);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        ensureNotClosed();
        return Collections.emptyMap();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
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

    public void updatePartitions(String topic, List<PartitionInfo> partitions) {
        ensureNotClosed();
        this.partitions.put(topic, partitions);
    }

    @Override
    public void pause(TopicPartition... partitions) {
        for (TopicPartition partition : partitions) {
            subscriptions.pause(partition);
            paused.add(partition);
        }
    }

    @Override
    public void resume(TopicPartition... partitions) {
        for (TopicPartition partition : partitions) {
            subscriptions.resume(partition);
            paused.remove(partition);
        }
    }

    @Override
    public void close() {
        ensureNotClosed();
        this.closed = true;
    }

    public boolean closed() {
        return this.closed;
    }

    @Override
    public void wakeup() {
        wakeup.set(true);
    }

    public void waitForPoll(long timeoutMs) {
        waitForPollThen(null, timeoutMs);
    }

    public void waitForPollThen(Runnable task, long timeoutMs) {
        CountDownLatch latch = new CountDownLatch(1);
        synchronized (latch) {
            pollLatch.set(latch);
            try {
                if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS))
                    throw new TimeoutException("Timed out waiting for consumer thread to call poll().");
            } catch (InterruptedException e) {
                throw new IllegalStateException("MockConsumer waiting thread was interrupted.", e);
            }
            if (task != null)
                task.run();
        }
    }

    public Set<TopicPartition> paused() {
        return Collections.unmodifiableSet(new HashSet<>(paused));
    }

    private void ensureNotClosed() {
        if (this.closed)
            throw new IllegalStateException("This consumer has already been closed.");
    }

    private void updateFetchPosition(TopicPartition tp) {
        if (subscriptions.isOffsetResetNeeded(tp)) {
            resetOffsetPosition(tp);
        } else if (subscriptions.committed(tp) == null) {
            subscriptions.needOffsetReset(tp);
            resetOffsetPosition(tp);
        } else {
            subscriptions.seek(tp, subscriptions.committed(tp).offset());
        }
    }

    private void resetOffsetPosition(TopicPartition tp) {
        OffsetResetStrategy strategy = subscriptions.resetStrategy(tp);
        Long offset;
        if (strategy == OffsetResetStrategy.EARLIEST) {
            offset = beginningOffsets.get(tp);
            if (offset == null)
                throw new IllegalStateException("MockConsumer didn't have beginning offset specified, but tried to seek to beginning");
        } else if (strategy == OffsetResetStrategy.LATEST) {
            offset = endOffsets.get(tp);
            if (offset == null)
                throw new IllegalStateException("MockConsumer didn't have end offset specified, but tried to seek to end");
        } else {
            throw new NoOffsetForPartitionException("No offset available");
        }
        seek(tp, offset);
    }
}

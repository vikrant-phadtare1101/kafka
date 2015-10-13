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
 **/

package org.apache.kafka.copycat.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest(KafkaBasedLog.class)
@PowerMockIgnore("javax.management.*")
public class KafkaBasedLogTest {

    private static final String TOPIC = "copycat-log";
    private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);
    private static final Map<String, Object> PRODUCER_PROPS = new HashMap<>();
    static {
        PRODUCER_PROPS.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093");
        PRODUCER_PROPS.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        PRODUCER_PROPS.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    }
    private static final Map<String, Object> CONSUMER_PROPS = new HashMap<>();
    static {
        CONSUMER_PROPS.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093");
        CONSUMER_PROPS.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        CONSUMER_PROPS.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    }

    private static final Set<TopicPartition> CONSUMER_ASSIGNMENT = new HashSet<>(Arrays.asList(TP0, TP1));
    private static final Map<String, String> FIRST_SET = new HashMap<>();
    static {
        FIRST_SET.put("key", "value");
        FIRST_SET.put(null, null);
    }

    private static final Node LEADER = new Node(1, "broker1", 9092);
    private static final Node REPLICA = new Node(1, "broker2", 9093);

    private static final PartitionInfo TPINFO0 = new PartitionInfo(TOPIC, 0, LEADER, new Node[]{REPLICA}, new Node[]{REPLICA});
    private static final PartitionInfo TPINFO1 = new PartitionInfo(TOPIC, 1, LEADER, new Node[]{REPLICA}, new Node[]{REPLICA});

    private static final String TP0_KEY = "TP0KEY";
    private static final String TP1_KEY = "TP1KEY";
    private static final String TP0_VALUE = "VAL0";
    private static final String TP1_VALUE = "VAL1";
    private static final String TP0_VALUE_NEW = "VAL0_NEW";
    private static final String TP1_VALUE_NEW = "VAL1_NEW";

    private Time time = new MockTime();
    private KafkaBasedLog<String, String> store;

    @Mock
    private KafkaProducer<String, String> producer;
    private MockConsumer<String, String> consumer;

    private List<ConsumerRecord<String, String>> consumedRecords = new ArrayList<>();
    private Callback<ConsumerRecord<String, String>> consumedCallback = new Callback<ConsumerRecord<String, String>>() {
        @Override
        public void onCompletion(Throwable error, ConsumerRecord<String, String> record) {
            consumedRecords.add(record);
        }
    };

    @Before
    public void setUp() throws Exception {
        store = PowerMock.createPartialMock(KafkaBasedLog.class, new String[]{"createConsumer", "createProducer"},
                TOPIC, PRODUCER_PROPS, CONSUMER_PROPS, consumedCallback, time);
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions(TOPIC, Arrays.asList(TPINFO0, TPINFO1));
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(TP0, 0L);
        beginningOffsets.put(TP1, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);
    }

    @Test
    public void testStartStop() throws Exception {
        expectStart();
        expectStop();

        PowerMock.replayAll();

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 0L);
        endOffsets.put(TP1, 0L);
        consumer.updateEndOffsets(endOffsets);
        store.start();
        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());

        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }

    @Test
    public void testReloadOnStart() throws Exception {
        expectStart();
        expectStop();

        PowerMock.replayAll();

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 1L);
        endOffsets.put(TP1, 1L);
        consumer.updateEndOffsets(endOffsets);
        Thread startConsumerOpsThread = new Thread("start-consumer-ops-thread") {
            @Override
            public void run() {
                // Needs to seek to end to find end offsets
                consumer.waitForPoll(10000);

                // Should keep polling until it reaches current log end offset for all partitions
                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, TP0_KEY, TP0_VALUE));
                    }
                }, 10000);

                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0, TP1_KEY, TP1_VALUE));
                    }
                }, 10000);
            }
        };
        startConsumerOpsThread.start();
        store.start();
        startConsumerOpsThread.join(10000);
        assertFalse(startConsumerOpsThread.isAlive());
        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());
        assertEquals(2, consumedRecords.size());
        assertEquals(TP0_VALUE, consumedRecords.get(0).value());
        assertEquals(TP1_VALUE, consumedRecords.get(1).value());

        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }

    @Test
    public void testSendAndReadToEnd() throws Exception {
        expectStart();
        TestFuture<RecordMetadata> tp0Future = new TestFuture<>();
        ProducerRecord<String, String> tp0Record = new ProducerRecord<>(TOPIC, TP0_KEY, TP0_VALUE);
        Capture<org.apache.kafka.clients.producer.Callback> callback0 = EasyMock.newCapture();
        EasyMock.expect(producer.send(EasyMock.eq(tp0Record), EasyMock.capture(callback0))).andReturn(tp0Future);
        TestFuture<RecordMetadata> tp1Future = new TestFuture<>();
        ProducerRecord<String, String> tp1Record = new ProducerRecord<>(TOPIC, TP1_KEY, TP1_VALUE);
        Capture<org.apache.kafka.clients.producer.Callback> callback1 = EasyMock.newCapture();
        EasyMock.expect(producer.send(EasyMock.eq(tp1Record), EasyMock.capture(callback1))).andReturn(tp1Future);

        // Producer flushes when read to log end is called
        producer.flush();
        PowerMock.expectLastCall();

        expectStop();

        PowerMock.replayAll();

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 0L);
        endOffsets.put(TP1, 0L);
        consumer.updateEndOffsets(endOffsets);
        Thread startConsumerOpsThread = new Thread("start-consumer-ops-thread") {
            @Override
            public void run() {
                // Should keep polling until it has partition info
                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.seek(TP0, 0);
                        consumer.seek(TP1, 0);
                    }
                }, 10000);
            }
        };
        startConsumerOpsThread.start();
        store.start();
        startConsumerOpsThread.join(10000);
        assertFalse(startConsumerOpsThread.isAlive());
        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());

        // Set some keys
        final AtomicInteger invoked = new AtomicInteger(0);
        org.apache.kafka.clients.producer.Callback producerCallback = new org.apache.kafka.clients.producer.Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                invoked.incrementAndGet();
            }
        };
        store.send(TP0_KEY, TP0_VALUE, producerCallback);
        store.send(TP1_KEY, TP1_VALUE, producerCallback);
        assertEquals(0, invoked.get());
        tp1Future.resolve((RecordMetadata) null); // Output not used, so safe to not return a real value for testing
        callback1.getValue().onCompletion(null, null);
        assertEquals(1, invoked.get());
        tp0Future.resolve((RecordMetadata) null);
        callback0.getValue().onCompletion(null, null);
        assertEquals(2, invoked.get());

        // Now we should have to wait for the records to be read back when we call readToEnd()
        final CountDownLatch startOffsetUpdateLatch = new CountDownLatch(1);
        Thread readNewDataThread = new Thread("read-new-data-thread") {
            @Override
            public void run() {
                // Needs to be woken up after calling readToEnd()
                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            startOffsetUpdateLatch.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException("Interrupted");
                        }
                    }
                }, 10000);

                // Needs to seek to end to find end offsets
                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            startOffsetUpdateLatch.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException("Interrupted");
                        }

                        Map<TopicPartition, Long> newEndOffsets = new HashMap<>();
                        newEndOffsets.put(TP0, 2L);
                        newEndOffsets.put(TP1, 2L);
                        consumer.updateEndOffsets(newEndOffsets);
                    }
                }, 10000);

                // Should keep polling until it reaches current log end offset for all partitions
                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, TP0_KEY, TP0_VALUE));
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, TP0_KEY, TP0_VALUE_NEW));
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0, TP1_KEY, TP1_VALUE));
                    }
                }, 10000);

                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 1, TP1_KEY, TP1_VALUE_NEW));
                    }
                }, 10000);
            }
        };
        readNewDataThread.start();
        final AtomicBoolean getInvokedAndPassed = new AtomicBoolean(false);
        FutureCallback<Void> readEndFutureCallback = new FutureCallback<>(new Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {
                assertEquals(4, consumedRecords.size());
                assertEquals(TP0_VALUE_NEW, consumedRecords.get(2).value());
                assertEquals(TP1_VALUE_NEW, consumedRecords.get(3).value());
                getInvokedAndPassed.set(true);
            }
        });
        store.readToEnd(readEndFutureCallback);
        startOffsetUpdateLatch.countDown();
        readNewDataThread.join(10000);
        assertFalse(readNewDataThread.isAlive());
        readEndFutureCallback.get(10000, TimeUnit.MILLISECONDS);
        assertTrue(getInvokedAndPassed.get());

        // Cleanup
        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }

    @Test
    public void testConsumerError() throws Exception {
        expectStart();
        expectStop();

        PowerMock.replayAll();

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 1L);
        endOffsets.put(TP1, 1L);
        consumer.updateEndOffsets(endOffsets);
        Thread startConsumerOpsThread = new Thread("start-consumer-ops-thread") {
            @Override
            public void run() {
                // Trigger exception
                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.setException(Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.exception());
                    }
                }, 10000);

                // Should keep polling until it reaches current log end offset for all partitions
                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, TP0_KEY, TP0_VALUE_NEW));
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0, TP0_KEY, TP0_VALUE_NEW));
                    }
                }, 10000);
            }
        };
        startConsumerOpsThread.start();
        store.start();
        startConsumerOpsThread.join(10000);
        assertFalse(startConsumerOpsThread.isAlive());
        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());

        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }

    @Test
    public void testProducerError() throws Exception {
        expectStart();
        TestFuture<RecordMetadata> tp0Future = new TestFuture<>();
        ProducerRecord<String, String> tp0Record = new ProducerRecord<>(TOPIC, TP0_KEY, TP0_VALUE);
        Capture<org.apache.kafka.clients.producer.Callback> callback0 = EasyMock.newCapture();
        EasyMock.expect(producer.send(EasyMock.eq(tp0Record), EasyMock.capture(callback0))).andReturn(tp0Future);

        expectStop();

        PowerMock.replayAll();

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 0L);
        endOffsets.put(TP1, 0L);
        consumer.updateEndOffsets(endOffsets);
        Thread startConsumerOpsThread = new Thread("start-consumer-ops-thread") {
            @Override
            public void run() {
                // Should keep polling until it has partition info
                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.seek(TP0, 0);
                        consumer.seek(TP1, 0);
                    }
                }, 10000);
            }
        };
        startConsumerOpsThread.start();
        store.start();
        startConsumerOpsThread.join(10000);
        assertFalse(startConsumerOpsThread.isAlive());
        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());

        final AtomicReference<Throwable> setException = new AtomicReference<>();
        store.send(TP0_KEY, TP0_VALUE, new org.apache.kafka.clients.producer.Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                assertNull(setException.get()); // Should only be invoked once
                setException.set(exception);
            }
        });
        KafkaException exc = new LeaderNotAvailableException("Error");
        tp0Future.resolve(exc);
        callback0.getValue().onCompletion(null, exc);
        assertNotNull(setException.get());

        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }


    private void expectStart() throws Exception {
        PowerMock.expectPrivate(store, "createProducer")
                .andReturn(producer);
        PowerMock.expectPrivate(store, "createConsumer")
                .andReturn(consumer);
    }

    private void expectStop() {
        producer.close();
        PowerMock.expectLastCall();
        // MockConsumer close is checked after test.
    }

    private static ByteBuffer buffer(String v) {
        return ByteBuffer.wrap(v.getBytes());
    }

}

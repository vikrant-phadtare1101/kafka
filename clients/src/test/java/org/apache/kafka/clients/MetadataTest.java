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
package org.apache.kafka.clients;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;

public class MetadataTest {

    private long refreshBackoffMs = 100;
    private long metadataExpireMs = 1000;
    private Metadata metadata = new Metadata(refreshBackoffMs, metadataExpireMs);
    private AtomicBoolean backgroundError = new AtomicBoolean(false);
    
    @After
    public void tearDown() {
        assertFalse(backgroundError.get());
    }

    @Test
    public void testMetadata() throws Exception {
        long time = 0;
        metadata.update(Cluster.empty(), time);
        assertFalse("No update needed.", metadata.timeToNextUpdate(time) == 0);
        metadata.requestUpdate();
        assertFalse("Still no updated needed due to backoff", metadata.timeToNextUpdate(time) == 0);
        time += refreshBackoffMs;
        assertTrue("Update needed now that backoff time expired", metadata.timeToNextUpdate(time) == 0);
        String topic = "my-topic";
        Thread t1 = asyncFetch(topic);
        Thread t2 = asyncFetch(topic);
        assertTrue("Awaiting update", t1.isAlive());
        assertTrue("Awaiting update", t2.isAlive());
        metadata.update(TestUtils.singletonCluster(topic, 1), time);
        t1.join();
        t2.join();
        assertFalse("No update needed.", metadata.timeToNextUpdate(time) == 0);
        time += metadataExpireMs;
        assertTrue("Update needed due to stale metadata.", metadata.timeToNextUpdate(time) == 0);
    }

    /**
     * Tests that {@link org.apache.kafka.clients.Metadata#awaitUpdate(int, long)} doesn't
     * wait forever with a max timeout value of 0
     *
     * @throws Exception
     * @see https://issues.apache.org/jira/browse/KAFKA-1836
     */
    @Test
    public void testMetadataUpdateWaitTime() throws Exception {
        long time = 0;
        metadata.update(Cluster.empty(), time);
        assertFalse("No update needed.", metadata.timeToNextUpdate(time) == 0);
        // first try with a max wait time of 0 and ensure that this returns back without waiting forever
        try {
            metadata.awaitUpdate(metadata.requestUpdate(), 0);
            fail("Wait on metadata update was expected to timeout, but it didn't");
        } catch (TimeoutException te) {
            // expected
        }
        // now try with a higher timeout value once
        final long twoSecondWait = 2000;
        try {
            metadata.awaitUpdate(metadata.requestUpdate(), twoSecondWait);
            fail("Wait on metadata update was expected to timeout, but it didn't");
        } catch (TimeoutException te) {
            // expected
        }
    }

    private Thread asyncFetch(final String topic) {
        Thread thread = new Thread() {
            public void run() {
                while (metadata.fetch().partitionsForTopic(topic) == null) {
                    try {
                        metadata.awaitUpdate(metadata.requestUpdate(), refreshBackoffMs);
                    } catch (Exception e) {
                        backgroundError.set(true);
                    }
                }
            }
        };
        thread.start();
        return thread;
    }
}

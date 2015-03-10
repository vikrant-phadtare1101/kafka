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

import org.apache.kafka.common.utils.MockTime;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HeartbeatTest {

    private long timeout = 300L;
    private MockTime time = new MockTime();
    private Heartbeat heartbeat = new Heartbeat(timeout, -1L);

    @Test
    public void testShouldHeartbeat() {
        heartbeat.sentHeartbeat(time.milliseconds());
        time.sleep((long) ((float) timeout / Heartbeat.HEARTBEATS_PER_SESSION_INTERVAL * 1.1));
        assertTrue(heartbeat.shouldHeartbeat(time.milliseconds()));
    }

    @Test
    public void testShouldNotHeartbeat() {
        heartbeat.sentHeartbeat(time.milliseconds());
        time.sleep(timeout / (2 * Heartbeat.HEARTBEATS_PER_SESSION_INTERVAL));
        assertFalse(heartbeat.shouldHeartbeat(time.milliseconds()));
    }
}

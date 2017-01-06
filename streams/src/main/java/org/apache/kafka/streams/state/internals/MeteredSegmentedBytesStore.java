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

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;

class MeteredSegmentedBytesStore implements SegmentedBytesStore {

    private final SegmentedBytesStore inner;
    private final String metricScope;
    private final Time time;

    private Sensor putTime;
    private Sensor fetchTime;
    private Sensor flushTime;
    private StreamsMetrics metrics;
    private Sensor getTime;
    private Sensor removeTime;

    MeteredSegmentedBytesStore(final SegmentedBytesStore inner, String metricScope, Time time) {
        this.inner = inner;
        this.metricScope = metricScope;
        this.time = time != null ? time : new SystemTime();
    }

    @Override
    public String name() {
        return inner.name();
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        final String name = name();
        this.metrics = context.metrics();
        this.putTime = this.metrics.addLatencySensor(metricScope, name, "put");
        this.fetchTime = this.metrics.addLatencySensor(metricScope, name, "fetch");
        this.flushTime = this.metrics.addLatencySensor(metricScope, name, "flush");
        this.getTime = this.metrics.addLatencySensor(metricScope, name, "get");
        this.removeTime = this.metrics.addLatencySensor(metricScope, name, "remove");

        final Sensor restoreTime = this.metrics.addLatencySensor(metricScope, name, "restore");
        // register and possibly restore the state from the logs
        final long startNs = time.nanoseconds();
        try {
            inner.init(context, root);
        } finally {
            this.metrics.recordLatency(restoreTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public boolean persistent() {
        return inner.persistent();
    }

    @Override
    public boolean isOpen() {
        return inner.isOpen();
    }

    @Override
    public byte[] get(final Bytes key) {
        final long startNs = time.nanoseconds();
        try {
            return inner.get(key);
        } finally {
            this.metrics.recordLatency(this.getTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetch(final Bytes key, long timeFrom, long timeTo) {
        return new MeteredSegmentedBytesStoreIterator(inner.fetch(key, timeFrom, timeTo), this.fetchTime);
    }

    @Override
    public void remove(final Bytes key) {
        final long startNs = time.nanoseconds();
        try {
            inner.remove(key);
        } finally {
            this.metrics.recordLatency(this.removeTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        long startNs = time.nanoseconds();
        try {
            this.inner.put(key, value);
        } finally {
            this.metrics.recordLatency(this.putTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public void flush() {
        final long startNs = time.nanoseconds();
        try {
            this.inner.flush();
        } finally {
            this.metrics.recordLatency(this.flushTime, startNs, time.nanoseconds());
        }
    }

    private class MeteredSegmentedBytesStoreIterator implements KeyValueIterator<Bytes, byte[]> {

        private final KeyValueIterator<Bytes, byte[]> iter;
        private final Sensor sensor;
        private final long startNs;

        MeteredSegmentedBytesStoreIterator(final KeyValueIterator<Bytes, byte[]> iter, Sensor sensor) {
            this.iter = iter;
            this.sensor = sensor;
            this.startNs = time.nanoseconds();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            return iter.next();
        }

        @Override
        public void remove() {
            iter.remove();
        }

        @Override
        public void close() {
            try {
                iter.close();
            } finally {
                metrics.recordLatency(this.sensor, this.startNs, time.nanoseconds());
            }
        }

        @Override
        public Bytes peekNextKey() {
            return iter.peekNextKey();
        }

    }

}

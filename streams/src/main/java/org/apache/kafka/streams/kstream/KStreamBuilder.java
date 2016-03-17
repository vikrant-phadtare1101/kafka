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

package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KStreamBuilder is a subclass of {@link TopologyBuilder} that provides the {@link KStream} DSL
 * for users to specify computational logic and translates the given logic to a processor topology.
 */
public class KStreamBuilder extends TopologyBuilder {

    private final AtomicInteger index = new AtomicInteger(0);

    public KStreamBuilder() {
        super();
    }

    /**
     * Creates a KStream instance for the specified topic.
     * The default deserializers specified in the config are used.
     *
     * @param topics          the topic names, if empty default to all the topics in the config
     * @return KStream
     */
    public <K, V> KStream<K, V> stream(String... topics) {
        return stream(null, null, topics);
    }

    /**
     * Creates a KStream instance for the specified topic.
     *
     * @param keySerde key serde used to read this source KStream,
     *                 if not specified the default serde defined in the configs will be used
     * @param valSerde value serde used to read this source KStream,
     *                 if not specified the default serde defined in the configs will be used
     * @param topics   the topic names, if empty default to all the topics in the config
     * @return KStream
     */
    public <K, V> KStream<K, V> stream(Serde<K> keySerde, Serde<V> valSerde, String... topics) {
        String name = newName(KStreamImpl.SOURCE_NAME);

        addSource(name, keySerde == null ? null : keySerde.deserializer(), valSerde == null ? null : valSerde.deserializer(), topics);

        return new KStreamImpl<>(this, name, Collections.singleton(name));
    }

    /**
     * Creates a KTable instance for the specified topic.
     * The default deserializers specified in the config are used.
     *
     * @param topic          the topic name
     * @return KTable
     */
    public <K, V> KTable<K, V> table(String topic) {
        return table(null, null, topic);
    }

    /**
     * Creates a KTable instance for the specified topic.
     *
     * @param keySerde   key serde used to send key-value pairs,
     *                        if not specified the default key serde defined in the configuration will be used
     * @param valSerde   value serde used to send key-value pairs,
     *                        if not specified the default value serde defined in the configuration will be used
     * @param topic          the topic name
     * @return KStream
     */
    public <K, V> KTable<K, V> table(Serde<K> keySerde, Serde<V> valSerde, String topic) {
        String source = newName(KStreamImpl.SOURCE_NAME);
        String name = newName(KTableImpl.SOURCE_NAME);

        addSource(source, keySerde == null ? null : keySerde.deserializer(), valSerde == null ? null : valSerde.deserializer(), topic);

        ProcessorSupplier<K, V> processorSupplier = new KTableSource<>(topic);
        addProcessor(name, processorSupplier, source);

        return new KTableImpl<>(this, name, processorSupplier, Collections.singleton(source), keySerde, valSerde);
    }

    /**
     * Creates a new stream by merging the given streams
     *
     * @param streams the streams to be merged
     * @return KStream
     */
    public <K, V> KStream<K, V> merge(KStream<K, V>... streams) {
        return KStreamImpl.merge(this, streams);
    }

    /**
     * Create a unique processor name used for translation into the processor topology.
     * This function is only for internal usage.
     *
     * @param prefix Processor name prefix.
     * @return The unique processor name.
     */
    public String newName(String prefix) {
        return prefix + String.format("%010d", index.getAndIncrement());
    }
}

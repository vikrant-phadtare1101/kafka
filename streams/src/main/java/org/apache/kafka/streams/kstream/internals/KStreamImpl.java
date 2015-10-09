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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamWindowed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.WindowSupplier;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.lang.reflect.Array;
import java.util.concurrent.atomic.AtomicInteger;

public class KStreamImpl<K, V> implements KStream<K, V> {

    private static final String FILTER_NAME = "KAFKA-FILTER-";

    private static final String MAP_NAME = "KAFKA-MAP-";

    private static final String MAPVALUES_NAME = "KAFKA-MAPVALUES-";

    private static final String FLATMAP_NAME = "KAFKA-FLATMAP-";

    private static final String FLATMAPVALUES_NAME = "KAFKA-FLATMAPVALUES-";

    private static final String TRANSFORM_NAME = "KAFKA-TRANSFORM-";

    private static final String TRANSFORMVALUES_NAME = "KAFKA-TRANSFORMVALUES-";

    private static final String PROCESSOR_NAME = "KAFKA-PROCESSOR-";

    private static final String BRANCH_NAME = "KAFKA-BRANCH-";

    private static final String BRANCHCHILD_NAME = "KAFKA-BRANCHCHILD-";

    private static final String WINDOWED_NAME = "KAFKA-WINDOWED-";

    private static final String SINK_NAME = "KAFKA-SINK-";

    public static final String JOINTHIS_NAME = "KAFKA-JOINTHIS-";

    public static final String JOINOTHER_NAME = "KAFKA-JOINOTHER-";

    public static final String JOINMERGE_NAME = "KAFKA-JOINMERGE-";

    public static final String SOURCE_NAME = "KAFKA-SOURCE-";

    public static final AtomicInteger INDEX = new AtomicInteger(1);

    protected final TopologyBuilder topology;
    protected final String name;

    public KStreamImpl(TopologyBuilder topology, String name) {
        this.topology = topology;
        this.name = name;
    }

    @Override
    public KStream<K, V> filter(Predicate<K, V> predicate) {
        String name = FILTER_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamFilter<>(predicate, false), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public KStream<K, V> filterOut(final Predicate<K, V> predicate) {
        String name = FILTER_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamFilter<>(predicate, true), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public <K1, V1> KStream<K1, V1> map(KeyValueMapper<K, V, KeyValue<K1, V1>> mapper) {
        String name = MAP_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamMap<>(mapper), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public <V1> KStream<K, V1> mapValues(ValueMapper<V, V1> mapper) {
        String name = MAPVALUES_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamMapValues<>(mapper), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K, V, Iterable<KeyValue<K1, V1>>> mapper) {
        String name = FLATMAP_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamFlatMap<>(mapper), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public <V1> KStream<K, V1> flatMapValues(ValueMapper<V, Iterable<V1>> mapper) {
        String name = FLATMAPVALUES_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamFlatMapValues<>(mapper), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public KStreamWindowed<K, V> with(WindowSupplier<K, V> windowSupplier) {
        String name = WINDOWED_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamWindow<>(windowSupplier), this.name);

        return new KStreamWindowedImpl<>(topology, name, windowSupplier);
    }

    @Override
    @SuppressWarnings("unchecked")
    public KStream<K, V>[] branch(Predicate<K, V>... predicates) {
        String branchName = BRANCH_NAME + INDEX.getAndIncrement();

        topology.addProcessor(branchName, new KStreamBranch(predicates.clone()), this.name);

        KStream<K, V>[] branchChildren = (KStream<K, V>[]) Array.newInstance(KStream.class, predicates.length);
        for (int i = 0; i < predicates.length; i++) {
            String childName = BRANCHCHILD_NAME + INDEX.getAndIncrement();

            topology.addProcessor(childName, new KStreamPassThrough<K, V>(), branchName);

            branchChildren[i] = new KStreamImpl<>(topology, childName);
        }

        return branchChildren;
    }

    @Override
    public <K1, V1> KStream<K1, V1> through(String topic,
                                            Serializer<K> keySerializer,
                                            Serializer<V> valSerializer,
                                            Deserializer<K1> keyDeserializer,
                                            Deserializer<V1> valDeserializer) {
        String sendName = SINK_NAME + INDEX.getAndIncrement();

        topology.addSink(sendName, topic, keySerializer, valSerializer, this.name);

        String sourceName = SOURCE_NAME + INDEX.getAndIncrement();

        topology.addSource(sourceName, keyDeserializer, valDeserializer, topic);

        return new KStreamImpl<>(topology, sourceName);
    }

    @Override
    public <K1, V1> KStream<K1, V1> through(String topic) {
        return through(topic, (Serializer<K>) null, (Serializer<V>) null, (Deserializer<K1>) null, (Deserializer<V1>) null);
    }

    @Override
    public void to(String topic) {
        String name = SINK_NAME + INDEX.getAndIncrement();

        topology.addSink(name, topic, this.name);
    }

    @Override
    public void to(String topic, Serializer<K> keySerializer, Serializer<V> valSerializer) {
        String name = SINK_NAME + INDEX.getAndIncrement();

        topology.addSink(name, topic, keySerializer, valSerializer, this.name);
    }

    @Override
    public <K1, V1> KStream<K1, V1> transform(TransformerSupplier<K, V, KeyValue<K1, V1>> transformerSupplier) {
        String name = TRANSFORM_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamTransform<>(transformerSupplier), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public <V1> KStream<K, V1> transformValues(ValueTransformerSupplier<V, V1> valueTransformerSupplier) {
        String name = TRANSFORMVALUES_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, new KStreamTransformValues<>(valueTransformerSupplier), this.name);

        return new KStreamImpl<>(topology, name);
    }

    @Override
    public void process(final ProcessorSupplier<K, V> processorSupplier) {
        String name = PROCESSOR_NAME + INDEX.getAndIncrement();

        topology.addProcessor(name, processorSupplier, this.name);
    }
}

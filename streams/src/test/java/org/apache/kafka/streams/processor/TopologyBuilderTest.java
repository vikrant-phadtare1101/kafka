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

package org.apache.kafka.streams.processor;

import static org.junit.Assert.assertEquals;

import static org.apache.kafka.common.utils.Utils.mkSet;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TopologyBuilderTest {

    @Test(expected = TopologyException.class)
    public void testAddSourceWithSameName() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", "topic-1");
        builder.addSource("source", "topic-2");
    }

    @Test(expected = TopologyException.class)
    public void testAddSourceWithSameTopic() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", "topic-1");
        builder.addSource("source-2", "topic-1");
    }

    @Test(expected = TopologyException.class)
    public void testAddProcessorWithSameName() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", "topic-1");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
    }

    @Test(expected = TopologyException.class)
    public void testAddProcessorWithWrongParent() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
    }

    @Test(expected = TopologyException.class)
    public void testAddProcessorWithSelfParent() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addProcessor("processor", new MockProcessorSupplier(), "processor");
    }

    @Test(expected = TopologyException.class)
    public void testAddSinkWithSameName() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", "topic-1");
        builder.addSink("sink", "topic-2", "source");
        builder.addSink("sink", "topic-3", "source");
    }

    @Test(expected = TopologyException.class)
    public void testAddSinkWithWrongParent() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSink("sink", "topic-2", "source");
    }

    @Test(expected = TopologyException.class)
    public void testAddSinkWithSelfParent() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSink("sink", "topic-2", "sink");
    }

    @Test
    public void testSourceTopics() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source-1", "topic-1");
        builder.addSource("source-2", "topic-2");
        builder.addSource("source-3", "topic-3");

        assertEquals(3, builder.sourceTopics().size());
    }

    @Test(expected = TopologyException.class)
    public void testAddStateStoreWithNonExistingProcessor() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addStateStore(new MockStateStoreSupplier("store", false), "no-such-processsor");
    }

    @Test(expected = TopologyException.class)
    public void testAddStateStoreWithSource() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source-1", "topic-1");
        builder.addStateStore(new MockStateStoreSupplier("store", false), "source-1");
    }

    @Test(expected = TopologyException.class)
    public void testAddStateStoreWithSink() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSink("sink-1", "topic-1");
        builder.addStateStore(new MockStateStoreSupplier("store", false), "sink-1");
    }

    @Test(expected = TopologyException.class)
    public void testAddStateStoreWithDuplicates() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addStateStore(new MockStateStoreSupplier("store", false));
        builder.addStateStore(new MockStateStoreSupplier("store", false));
    }

    @Test
    public void testAddStateStore() {
        final TopologyBuilder builder = new TopologyBuilder();
        List<StateStoreSupplier> suppliers;

        StateStoreSupplier supplier = new MockStateStoreSupplier("store-1", false);
        builder.addStateStore(supplier);
        suppliers = builder.build().stateStoreSuppliers();
        assertEquals(0, suppliers.size());

        builder.addSource("source-1", "topic-1");
        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");
        builder.connectProcessorAndStateStores("processor-1", "store-1");
        suppliers = builder.build().stateStoreSuppliers();
        assertEquals(1, suppliers.size());
        assertEquals(supplier.name(), suppliers.get(0).name());
    }

    @Test
    public void testTopicGroups() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source-1", "topic-1", "topic-1x");
        builder.addSource("source-2", "topic-2");
        builder.addSource("source-3", "topic-3");
        builder.addSource("source-4", "topic-4");
        builder.addSource("source-5", "topic-5");

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");

        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source-2", "processor-1");
        builder.copartitionSources(list("source-1", "source-2"));

        builder.addProcessor("processor-3", new MockProcessorSupplier(), "source-3", "source-4");

        Map<Integer, Set<String>> topicGroups = builder.topicGroups();

        Map<Integer, Set<String>> expectedTopicGroups = new HashMap<>();
        expectedTopicGroups.put(0, set("topic-1", "topic-1x", "topic-2"));
        expectedTopicGroups.put(1, set("topic-3", "topic-4"));
        expectedTopicGroups.put(2, set("topic-5"));

        assertEquals(3, topicGroups.size());
        assertEquals(expectedTopicGroups, topicGroups);

        Collection<Set<String>> copartitionGroups = builder.copartitionGroups();

        assertEquals(mkSet(mkSet("topic-1", "topic-1x", "topic-2")), new HashSet<>(copartitionGroups));
    }

    @Test
    public void testTopicGroupsByStateStore() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source-1", "topic-1", "topic-1x");
        builder.addSource("source-2", "topic-2");
        builder.addSource("source-3", "topic-3");
        builder.addSource("source-4", "topic-4");
        builder.addSource("source-5", "topic-5");

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");
        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source-2");
        builder.addStateStore(new MockStateStoreSupplier("strore-1", false), "processor-1", "processor-2");

        builder.addProcessor("processor-3", new MockProcessorSupplier(), "source-3");
        builder.addProcessor("processor-4", new MockProcessorSupplier(), "source-4");
        builder.addStateStore(new MockStateStoreSupplier("strore-2", false), "processor-3", "processor-4");

        Map<Integer, Set<String>> topicGroups = builder.topicGroups();

        Map<Integer, Set<String>> expectedTopicGroups = new HashMap<>();
        expectedTopicGroups.put(0, set("topic-1", "topic-1x", "topic-2"));
        expectedTopicGroups.put(1, set("topic-3", "topic-4"));
        expectedTopicGroups.put(2, set("topic-5"));

        assertEquals(3, topicGroups.size());
        assertEquals(expectedTopicGroups, topicGroups);
    }

    private <T> Set<T> set(T... items) {
        Set<T> set = new HashSet<>();
        for (T item : items) {
            set.add(item);
        }
        return set;
    }

    private <T> List<T> list(T... elems) {
        return Arrays.asList(elems);
    }

}

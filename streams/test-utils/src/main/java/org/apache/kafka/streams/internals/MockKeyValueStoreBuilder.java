/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.internals;

import org.apache.kafka.common.serialization.Serde;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.AbstractStoreBuilder;



public class MockKeyValueStoreBuilder<K, V> extends AbstractStoreBuilder<K, V, KeyValueStore> {

    private final boolean persistent;
    private final KeyValueBytesStoreSupplier storeSupplier;

    public MockKeyValueStoreBuilder(final KeyValueBytesStoreSupplier storeSupplier,
                                    final Serde<K> keySerde,
                                    final Serde<V> valueSerde,
                                    final boolean persistent) {
        super(storeSupplier.name(), keySerde, valueSerde, Time.SYSTEM);
        this.persistent = persistent;
        this.storeSupplier = storeSupplier;
    }

    @Override
    public KeyValueStore build() {
        return new MockKeyValueStore<>(name, storeSupplier.get(), persistent);
    }
}


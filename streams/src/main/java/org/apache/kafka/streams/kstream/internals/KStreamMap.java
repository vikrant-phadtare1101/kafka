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

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.processor.ProcessorSupplier;

class KStreamMap<K1, V1, K2, V2> implements ProcessorSupplier<K1, V1> {

    private final KeyValueMapper<K1, V1, KeyValue<K2, V2>> mapper;

    public KStreamMap(KeyValueMapper<K1, V1, KeyValue<K2, V2>> mapper) {
        this.mapper = mapper;
    }

    @Override
    public Processor<K1, V1> get() {
        return new KStreamMapProcessor();
    }

    private class KStreamMapProcessor extends AbstractProcessor<K1, V1> {
        @Override
        public void process(K1 key, V1 value) {
            KeyValue<K2, V2> newPair = mapper.apply(key, value);
            context().forward(newPair.key, newPair.value);
        }
    }
}

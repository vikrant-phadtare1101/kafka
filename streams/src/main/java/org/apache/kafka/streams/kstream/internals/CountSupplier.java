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

import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.AggregatorSupplier;

public class CountSupplier<K, V> implements AggregatorSupplier<K, V, Long> {

    private class Count implements Aggregator<K, V, Long> {

        @Override
        public Long initialValue() {
            return 0L;
        }

        @Override
        public Long add(K aggKey, V value, Long aggregate) {
            return aggregate + 1;
        }

        @Override
        public Long remove(K aggKey, V value, Long aggregate) {
            return aggregate - 1;
        }

        @Override
        public Long merge(Long aggr1, Long aggr2) {
            return aggr1 + aggr2;
        }
    }

    @Override
    public Aggregator<K, V, Long> get() {
        return new Count();
    }
}
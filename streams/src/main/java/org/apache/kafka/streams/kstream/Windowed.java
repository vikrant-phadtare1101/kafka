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

/**
 * The windowed key interface used in {@link KTable}, used for representing a windowed table result from windowed stream aggregations,
 * i.e. {@link KStream#aggregateByKey(Initializer, Aggregator, Windows, org.apache.kafka.common.serialization.Serializer,
 * org.apache.kafka.common.serialization.Serializer, org.apache.kafka.common.serialization.Deserializer,
 * org.apache.kafka.common.serialization.Deserializer)}
 *
 * @param <T> Type of the key
 */
public class Windowed<T> {

    private T value;

    private Window window;

    public Windowed(T value, Window window) {
        this.value = value;
        this.window = window;
    }

    public T value() {
        return value;
    }

    public Window window() {
        return window;
    }

    @Override
    public String toString() {
        return "[" + value + "@" + window.start() + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof Windowed))
            return false;

        Windowed<?> that = (Windowed) obj;

        return this.window.equals(that.window) && this.value.equals(that.value);
    }

    @Override
    public int hashCode() {
        long n = ((long) window.hashCode() << 32) | value.hashCode();
        return (int) (n % 0xFFFFFFFFL);
    }
}

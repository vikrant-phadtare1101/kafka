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
 * A single window instance, defined by its start and end timestamp.
 */
public abstract class Window {

    private long start;
    private long end;

    public Window(long start, long end) {
        this.start = start;
        this.end = end;
    }

    /**
     * Returns the start timestamp of this window, inclusive
     */
    public long start() {
        return start;
    }

    /**
     * Returns the end timestamp of this window, exclusive
     */
    public long end() {
        return end;
    }

    public boolean overlap(Window other) {
        return this.start() < other.end() || other.start() < this.end();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        Window other = (Window) obj;
        return this.start == other.start && this.end == other.end;
    }

    @Override
    public int hashCode() {
        long n = (this.start << 32) | this.end;
        return (int) (n % 0xFFFFFFFFL);
    }

}
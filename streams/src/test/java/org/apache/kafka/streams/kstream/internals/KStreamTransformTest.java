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

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KStreamTransformTest {

    private String topicName = "topic";

    private IntegerDeserializer keyDeserializer = new IntegerDeserializer();
    private IntegerDeserializer valDeserializer = new IntegerDeserializer();

    @Test
    public void testTransform() {
        KStreamBuilder builder = new KStreamBuilder();

        TransformerSupplier<Integer, Integer, KeyValue<Integer, Integer>> transformerSupplier =
            new TransformerSupplier<Integer, Integer, KeyValue<Integer, Integer>>() {
                public Transformer<Integer, Integer, KeyValue<Integer, Integer>> get() {
                    return new Transformer<Integer, Integer, KeyValue<Integer, Integer>>() {

                        private int total = 0;

                        @Override
                        public void init(ProcessorContext context) {
                        }

                        @Override
                        public KeyValue<Integer, Integer> transform(Integer key, Integer value) {
                            total += value;
                            return KeyValue.pair(key * 2, total);
                        }

                        @Override
                        public void punctuate(long timestamp) {
                        }

                        @Override
                        public void close() {
                        }
                    };
                }
            };

        final int[] expectedKeys = {1, 10, 100, 1000};

        KStream<Integer, Integer> stream;
        MockProcessorSupplier<Integer, Integer> processor = new MockProcessorSupplier<>();
        stream = builder.from(keyDeserializer, valDeserializer, topicName);
        stream.transform(transformerSupplier).process(processor);

        KStreamTestDriver driver = new KStreamTestDriver(builder);
        for (int i = 0; i < expectedKeys.length; i++) {
            driver.process(topicName, expectedKeys[i], expectedKeys[i] * 10);
        }

        assertEquals(4, processor.processed.size());

        String[] expected = {"2:10", "20:110", "200:1110", "2000:11110"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }

}

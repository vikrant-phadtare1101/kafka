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

import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class KStreamTransform<K1, V1, K2, V2> implements ProcessorSupplier<K1, V1> {

    private final TransformerSupplier<K1, V1, KeyValue<K2, V2>> transformerSupplier;

    public KStreamTransform(TransformerSupplier<K1, V1, KeyValue<K2, V2>> transformerSupplier) {
        this.transformerSupplier = transformerSupplier;
    }

    @Override
    public Processor<K1, V1> get() {
        return new KStreamTransformProcessor(transformerSupplier.get());
    }

    public static class KStreamTransformProcessor<K1, V1, K2, V2> implements Processor<K1, V1> {

        private final Transformer<K1, V1, KeyValue<K2, V2>> transformer;
        private ProcessorContext context;

        public KStreamTransformProcessor(Transformer<K1, V1, KeyValue<K2, V2>> transformer) {
            this.transformer = transformer;
        }

        @Override
        public void init(ProcessorContext context) {
            transformer.init(context);
            this.context = context;
        }

        @Override
        public void process(K1 key, V1 value) {
            KeyValue<K2, V2> pair = transformer.transform(key, value);
            context.forward(pair.key, pair.value);
        }

        @Override
        public void punctuate(long timestamp) {
            transformer.punctuate(timestamp);
        }

        @Override
        public void close() {
            transformer.close();
        }
    }
}

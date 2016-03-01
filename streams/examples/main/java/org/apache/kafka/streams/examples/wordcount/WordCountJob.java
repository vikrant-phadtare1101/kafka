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

package org.apache.kafka.streams.examples.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Properties;

/**
 * Demonstrates, using the high-level KStream DSL, how to implement the WordCount program
 * that computes a simple word occurrence histogram from an input text.
 *
 * In this example, the input stream reads from a topic named "streams-file-input", where the values of messages
 * represent lines of text; and the histogram output is written to topic "streams-wordcount-output" where each record
 * is an updated count of a single word.
 *
 * Before running this example you must create the source topic (e.g. via bin/kafka-topics.sh --create ...)
 * and write some data to it (e.g. via bin-kafka-console-producer.sh). Otherwise you won't see any data arriving in the output topic.
 */
public class WordCountJob {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.JOB_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(StreamsConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        final Serializer<String> stringSerializer = new StringSerializer();
        final Deserializer<String> stringDeserializer = new StringDeserializer();
        final Serializer<Long> longSerializer = new LongSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();

        KStream<String, String> source = builder.stream("streams-file-input");

        KTable<String, Long> counts = source
                .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                    @Override
                    public Iterable<String> apply(String value) {
                        return Arrays.asList(value.toLowerCase().split(" "));
                    }
                }).map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
                    @Override
                    public KeyValue<String, String> apply(String key, String value) {
                        return new KeyValue<String, String>(value, value);
                    }
                })
                .countByKey(stringSerializer, longSerializer, stringDeserializer, longDeserializer, "Counts");

        counts.to("streams-wordcount-output", stringSerializer, longSerializer);

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // usually the streaming job would be ever running,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(5000L);

        streams.close();
    }
}

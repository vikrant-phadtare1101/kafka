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
package org.apache.kafka.streams.integration;

import kafka.log.LogConfig;
import kafka.utils.MockTime;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForCompletion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests related to internal topics in streams
 */
@Category({IntegrationTest.class})
public class InternalTopicIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final String APP_ID = "internal-topics-integration-test";
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";

    private final MockTime mockTime = CLUSTER.time;

    private Properties streamsProp;

    @BeforeClass
    public static void startKafkaCluster() throws InterruptedException {
        CLUSTER.createTopics(DEFAULT_INPUT_TOPIC);
    }

    @Before
    public void before() {
        streamsProp = new Properties();
        streamsProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsProp.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsProp.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        streamsProp.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsProp.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    }

    @After
    public void after() throws IOException {
        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsProp);
    }

    private void produceData(final List<String> inputValues) throws Exception {
        final Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProp.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProp.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        IntegrationTestUtils.produceValuesSynchronously(DEFAULT_INPUT_TOPIC, inputValues, producerProp, mockTime);
    }

    private Properties getTopicProperties(final String changelog) {
        try (final AdminClient adminClient = createAdminClient()) {
            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, changelog);
            try {
                final Config config = adminClient.describeConfigs(Collections.singletonList(configResource)).values().get(configResource).get();
                final Properties properties = new Properties();
                for (final ConfigEntry configEntry : config.entries()) {
                    if (configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                        properties.put(configEntry.name(), configEntry.value());
                    }
                }
                return properties;
            } catch (final InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private AdminClient createAdminClient() {
        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        return AdminClient.create(adminClientConfig);
    }

    @Test
    public void shouldCompactTopicsForKeyValueStoreChangelogs() throws Exception {
        final String appID = APP_ID + "-compact";
        streamsProp.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        //
        // Step 1: Configure and start a simple word count topology
        //
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines = builder.stream(DEFAULT_INPUT_TOPIC);

        textLines.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(final String value) {
                return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
            }
        })
                .groupBy(MockMapper.<String, String>selectValueMapper())
                .count(Materialized.<String, Long, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("Counts"));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProp);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        produceData(Arrays.asList("hello", "world", "world", "hello world"));

        //
        // Step 3: Verify the state changelog topics are compact
        //
        waitForCompletion(streams, 2, 10000);
        streams.close();

        final Properties changelogProps = getTopicProperties(ProcessorStateManager.storeChangelogTopic(appID, "Counts"));
        assertEquals(LogConfig.Compact(), changelogProps.getProperty(LogConfig.CleanupPolicyProp()));

        final Properties repartitionProps = getTopicProperties(appID + "-Counts-repartition");
        assertEquals(LogConfig.Delete(), repartitionProps.getProperty(LogConfig.CleanupPolicyProp()));
        assertEquals(5, repartitionProps.size());
    }

    @Test
    public void shouldCompactAndDeleteTopicsForWindowStoreChangelogs() throws Exception {
        final String appID = APP_ID + "-compact-delete";
        streamsProp.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        //
        // Step 1: Configure and start a simple word count topology
        //
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines = builder.stream(DEFAULT_INPUT_TOPIC);

        final int durationMs = 2000;

        textLines.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(final String value) {
                return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
            }
        })
            .groupBy(MockMapper.<String, String>selectValueMapper())
            .windowedBy(TimeWindows.of(1000).grace(0L))
            .count(
                Materialized
                    .<String, Long, WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("CountWindows")
                    .withRetention(2_000L)
            );

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProp);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        produceData(Arrays.asList("hello", "world", "world", "hello world"));

        //
        // Step 3: Verify the state changelog topics are compact
        //
        waitForCompletion(streams, 2, 5000);
        streams.close();
        final Properties properties = getTopicProperties(ProcessorStateManager.storeChangelogTopic(appID, "CountWindows"));
        final List<String> policies = Arrays.asList(properties.getProperty(LogConfig.CleanupPolicyProp()).split(","));
        assertEquals(2, policies.size());
        assertTrue(policies.contains(LogConfig.Compact()));
        assertTrue(policies.contains(LogConfig.Delete()));
        // retention should be 1 day + the window duration
        final long retention = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS) + durationMs;
        assertEquals(retention, Long.parseLong(properties.getProperty(LogConfig.RetentionMsProp())));

        final Properties repartitionProps = getTopicProperties(appID + "-CountWindows-repartition");
        assertEquals(LogConfig.Delete(), repartitionProps.getProperty(LogConfig.CleanupPolicyProp()));
        assertEquals(5, repartitionProps.size());
    }
}

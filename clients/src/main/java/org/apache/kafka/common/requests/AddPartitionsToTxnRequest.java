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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AddPartitionsToTxnRequest extends AbstractRequest {
    private static final String TRANSACTIONAL_ID_KEY_NAME = "transactional_id";
    private static final String PID_KEY_NAME = "producer_id";
    private static final String EPOCH_KEY_NAME = "producer_epoch";
    private static final String TOPIC_PARTITIONS_KEY_NAME = "topics";
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    public static class Builder extends AbstractRequest.Builder<AddPartitionsToTxnRequest> {
        private final String transactionalId;
        private final long producerId;
        private final short producerEpoch;
        private final List<TopicPartition> partitions;

        public Builder(String transactionalId, long producerId, short producerEpoch, List<TopicPartition> partitions) {
            super(ApiKeys.ADD_PARTITIONS_TO_TXN);
            this.transactionalId = transactionalId;
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.partitions = partitions;
        }

        @Override
        public AddPartitionsToTxnRequest build(short version) {
            return new AddPartitionsToTxnRequest(version, transactionalId, producerId, producerEpoch, partitions);
        }
    }

    private final String transactionalId;
    private final long producerId;
    private final short producerEpoch;
    private final List<TopicPartition> partitions;

    private AddPartitionsToTxnRequest(short version, String transactionalId, long producerId, short producerEpoch,
                                      List<TopicPartition> partitions) {
        super(version);
        this.transactionalId = transactionalId;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.partitions = partitions;
    }

    public AddPartitionsToTxnRequest(Struct struct, short version) {
        super(version);
        this.transactionalId = struct.getString(TRANSACTIONAL_ID_KEY_NAME);
        this.producerId = struct.getLong(PID_KEY_NAME);
        this.producerEpoch = struct.getShort(EPOCH_KEY_NAME);

        List<TopicPartition> partitions = new ArrayList<>();
        Object[] topicPartitionsArray = struct.getArray(TOPIC_PARTITIONS_KEY_NAME);
        for (Object topicPartitionObj : topicPartitionsArray) {
            Struct topicPartitionStruct = (Struct) topicPartitionObj;
            String topic = topicPartitionStruct.getString(TOPIC_KEY_NAME);
            for (Object partitionObj : topicPartitionStruct.getArray(PARTITIONS_KEY_NAME)) {
                partitions.add(new TopicPartition(topic, (Integer) partitionObj));
            }
        }
        this.partitions = partitions;
    }

    public String transactionalId() {
        return transactionalId;
    }

    public long producerId() {
        return producerId;
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public List<TopicPartition> partitions() {
        return partitions;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.ADD_PARTITIONS_TO_TXN.requestSchema(version()));
        struct.set(TRANSACTIONAL_ID_KEY_NAME, transactionalId);
        struct.set(PID_KEY_NAME, producerId);
        struct.set(EPOCH_KEY_NAME, producerEpoch);

        Map<String, List<Integer>> mappedPartitions = CollectionUtils.groupDataByTopic(partitions);
        Object[] partitionsArray = new Object[mappedPartitions.size()];
        int i = 0;
        for (Map.Entry<String, List<Integer>> topicAndPartitions : mappedPartitions.entrySet()) {
            Struct topicPartitionsStruct = struct.instance(TOPIC_PARTITIONS_KEY_NAME);
            topicPartitionsStruct.set(TOPIC_KEY_NAME, topicAndPartitions.getKey());
            topicPartitionsStruct.set(PARTITIONS_KEY_NAME, topicAndPartitions.getValue().toArray());
            partitionsArray[i++] = topicPartitionsStruct;
        }

        struct.set(TOPIC_PARTITIONS_KEY_NAME, partitionsArray);
        return struct;
    }

    @Override
    public AddPartitionsToTxnResponse getErrorResponse(Throwable e) {
        return new AddPartitionsToTxnResponse(Errors.forException(e));
    }

    public static AddPartitionsToTxnRequest parse(ByteBuffer buffer, short version) {
        return new AddPartitionsToTxnRequest(ApiKeys.ADD_PARTITIONS_TO_TXN.parseRequest(version, buffer), version);
    }

}

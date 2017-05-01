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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails;

import java.util.List;
import java.util.Map;

/**
 * A request to create a new topic through the AdminClient API. 
 */
public class NewTopic {
    private final String name;
    private final int numPartitions;
    private final short replicationFactor;
    private final Map<Integer, List<Integer>> replicasAssignments;
    private Map<String, String> configs = null;

    /**
     * Create a new topic with a fixed replication factor and number of partitions.
     */
    public NewTopic(String name, int numPartitions, short replicationFactor) {
        this.name = name;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        this.replicasAssignments = null;
    }

    /**
     * A request to create a new topic with a specific replica assignment configuration.
     */
    public NewTopic(String name, Map<Integer, List<Integer>> replicasAssignments) {
        this.name = name;
        this.numPartitions = -1;
        this.replicationFactor = -1;
        this.replicasAssignments = replicasAssignments;
    }

    public String name() {
        return name;
    }

    /**
     * Set the configuration to use on the new topic.
     *
     * @param configs               The configuration map.
     * @return                      This NewTopic object.
     */
    public NewTopic configs(Map<String, String> configs) {
        this.configs = configs;
        return this;
    }

    TopicDetails convertToTopicDetails() {
        if (replicasAssignments != null) {
            if (configs != null) {
                return new TopicDetails(replicasAssignments, configs);
            } else {
                return new TopicDetails(replicasAssignments);
            }
        } else {
            if (configs != null) {
                return new TopicDetails(numPartitions, replicationFactor, configs);
            } else {
                return new TopicDetails(numPartitions, replicationFactor);
            }
        }
    }
}

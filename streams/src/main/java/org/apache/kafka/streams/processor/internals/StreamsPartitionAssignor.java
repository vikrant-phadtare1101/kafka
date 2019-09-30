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
package org.apache.kafka.streams.processor.internals;

import java.util.Iterator;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.PartitionGrouper;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.streams.processor.internals.assignment.CopartitionedTopicsEnforcer;
import org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.EARLIEST_PROBEABLE_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.UNKNOWN;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.VERSION_FIVE;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.VERSION_FOUR;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.VERSION_THREE;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.VERSION_TWO;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.VERSION_ONE;

public class StreamsPartitionAssignor implements ConsumerPartitionAssignor, Configurable {
    private Logger log;
    private String logPrefix;

    private static class AssignedPartition implements Comparable<AssignedPartition> {
        private final TaskId taskId;
        private final TopicPartition partition;

        AssignedPartition(final TaskId taskId,
                          final TopicPartition partition) {
            this.taskId = taskId;
            this.partition = partition;
        }

        @Override
        public int compareTo(final AssignedPartition that) {
            return PARTITION_COMPARATOR.compare(partition, that.partition);
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof AssignedPartition)) {
                return false;
            }
            final AssignedPartition other = (AssignedPartition) o;
            return compareTo(other) == 0;
        }

        @Override
        public int hashCode() {
            // Only partition is important for compareTo, equals and hashCode.
            return partition.hashCode();
        }
    }

    private static class ClientMetadata {
        private final HostInfo hostInfo;
        private final Set<String> consumers;
        private final ClientState state;

        ClientMetadata(final String endPoint) {

            // get the host info if possible
            if (endPoint != null) {
                final String host = getHost(endPoint);
                final Integer port = getPort(endPoint);

                if (host == null || port == null) {
                    throw new ConfigException(
                        String.format("Error parsing host address %s. Expected format host:port.", endPoint)
                    );
                }

                hostInfo = new HostInfo(host, port);
            } else {
                hostInfo = null;
            }

            // initialize the consumer memberIds
            consumers = new HashSet<>();

            // initialize the client state
            state = new ClientState();
        }

        void addConsumer(final String consumerMemberId,
                         final SubscriptionInfo info,
                         final List<TopicPartition> ownedPartitions) {
            consumers.add(consumerMemberId);
            state.incrementCapacity();
            state.addPreviousStandbyTasks(info.standbyTasks());
            state.addPreviousActiveTasks(info.prevTasks());
            state.addOwnedPartitions(ownedPartitions, consumerMemberId);
        }

        @Override
        public String toString() {
            return "ClientMetadata{" +
                "hostInfo=" + hostInfo +
                ", consumers=" + consumers +
                ", state=" + state +
                '}';
        }
    }


    protected static final Comparator<TopicPartition> PARTITION_COMPARATOR =
        Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    private String userEndPoint;
    private int numStandbyReplicas;

    // heuristic to favor balance over stickiness if we know a second rebalance will be required anyway
    private boolean rebalanceRequired = false;

    private TaskManager taskManager;
    private PartitionGrouper partitionGrouper;
    private AtomicInteger assignmentErrorCode;

    protected int usedSubscriptionMetadataVersion = LATEST_SUPPORTED_VERSION;

    private InternalTopicManager internalTopicManager;
    private CopartitionedTopicsEnforcer copartitionedTopicsEnforcer;
    private RebalanceProtocol rebalanceProtocol;

    protected String userEndPoint() {
        return userEndPoint;
    }

    protected TaskManager taskManger() {
        return taskManager;
    }

    /**
     * We need to have the PartitionAssignor and its StreamThread to be mutually accessible
     * since the former needs later's cached metadata while sending subscriptions,
     * and the latter needs former's returned assignment when adding tasks.
     *
     * @throws KafkaException if the stream thread is not specified
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        final AssignorConfiguration assignorConfiguration = new AssignorConfiguration(configs);

        logPrefix = assignorConfiguration.logPrefix();
        log = new LogContext(logPrefix).logger(getClass());
        usedSubscriptionMetadataVersion = assignorConfiguration.configuredMetadataVersion(usedSubscriptionMetadataVersion);
        taskManager = assignorConfiguration.getTaskManager();
        assignmentErrorCode = assignorConfiguration.getAssignmentErrorCode(configs);
        numStandbyReplicas = assignorConfiguration.getNumStandbyReplicas();
        partitionGrouper = assignorConfiguration.getPartitionGrouper();
        userEndPoint = assignorConfiguration.getUserEndPoint();
        internalTopicManager = assignorConfiguration.getInternalTopicManager();
        copartitionedTopicsEnforcer = assignorConfiguration.getCopartitionedTopicsEnforcer();
        rebalanceProtocol = assignorConfiguration.rebalanceProtocol();
    }

    @Override
    public String name() {
        return "stream";
    }

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        final List<RebalanceProtocol> supportedProtocols = new ArrayList<>();
        supportedProtocols.add(RebalanceProtocol.EAGER);
        if (rebalanceProtocol == RebalanceProtocol.COOPERATIVE) {
            supportedProtocols.add(rebalanceProtocol);
        }
        return supportedProtocols;
    }

    @Override
    public ByteBuffer subscriptionUserData(final Set<String> topics) {
        // Adds the following information to subscription
        // 1. Client UUID (a unique id assigned to an instance of KafkaStreams)
        // 2. Task ids of previously running tasks
        // 3. Task ids of valid local states on the client's state directory.

        // Any tasks that are not yet running are counted as standby tasks for assignment purposes, along with any old
        // tasks for which we still found state on disk
        final Set<TaskId> activeTasks;
        final Set<TaskId> standbyTasks = taskManager.cachedTasksIds();

        // In eager, onPartitionsRevoked is called first and we must get the previously saved running task ids
        if (rebalanceProtocol == RebalanceProtocol.EAGER) {
            activeTasks = taskManager.previousRunningTaskIds();
            standbyTasks.removeAll(activeTasks);

        // In cooperative, we will use the encoded ownedPartitions to determine the running tasks
        } else if (rebalanceProtocol == RebalanceProtocol.COOPERATIVE) {
            activeTasks = Collections.emptySet();
            standbyTasks.removeAll(taskManager.activeTaskIds());
        } else {
            throw new  IllegalStateException("Streams partition assignor has no known rebalance protocol");
        }

        final SubscriptionInfo data = new SubscriptionInfo(
            usedSubscriptionMetadataVersion,
            taskManager.processId(),
            activeTasks,
            standbyTasks,
            userEndPoint);

        taskManager.updateSubscriptionsFromMetadata(topics);
        taskManager.setRebalanceInProgress(true);

        return data.encode();
    }

    private Map<String, Assignment> errorAssignment(final Map<UUID, ClientMetadata> clientsMetadata,
                                                    final String topic,
                                                    final int errorCode) {
        log.error("{} is unknown yet during rebalance," +
                      " please make sure they have been pre-created before starting the Streams application.", topic);
        final Map<String, Assignment> assignment = new HashMap<>();
        for (final ClientMetadata clientMetadata : clientsMetadata.values()) {
            for (final String consumerId : clientMetadata.consumers) {
                assignment.put(consumerId, new Assignment(
                    Collections.emptyList(),
                    new AssignmentInfo(LATEST_SUPPORTED_VERSION,
                                       Collections.emptyList(),
                                       Collections.emptyMap(),
                                       Collections.emptyMap(),
                                       errorCode).encode()
                ));
            }
        }
        return assignment;
    }

    /*
     * This assigns tasks to consumer clients in the following steps.
     *
     * 0. check all repartition source topics and use internal topic manager to make sure
     *    they have been created with the right number of partitions.
     *
     * 1. using user customized partition grouper to generate tasks along with their
     *    assigned partitions; also make sure that the task's corresponding changelog topics
     *    have been created with the right number of partitions.
     *
     * 2. using TaskAssignor to assign tasks to consumer clients.
     *    - Assign a task to a client which was running it previously.
     *      If there is no such client, assign a task to a client which has its valid local state.
     *    - A client may have more than one stream threads.
     *      The assignor tries to assign tasks to a client proportionally to the number of threads.
     *    - We try not to assign the same set of tasks to two different clients
     *    We do the assignment in one-pass. The result may not satisfy above all.
     *
     * 3. within each client, tasks are assigned to consumer clients in round-robin manner.
     */
    @Override
    public GroupAssignment assign(final Cluster metadata, final GroupSubscription groupSubscription) {
        final Map<String, Subscription> subscriptions = groupSubscription.groupSubscription();
        // construct the client metadata from the decoded subscription info
        final Map<UUID, ClientMetadata> clientMetadataMap = new HashMap<>();
        final Set<String> futureConsumers = new HashSet<>();
        final Set<TopicPartition> allOwnedPartitions = new HashSet<>();

        int minReceivedMetadataVersion = LATEST_SUPPORTED_VERSION;

        int futureMetadataVersion = UNKNOWN;
        for (final Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            final String consumerId = entry.getKey();
            final Subscription subscription = entry.getValue();

            final SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());
            final int usedVersion = info.version();
            if (usedVersion > LATEST_SUPPORTED_VERSION) {
                futureMetadataVersion = usedVersion;
                futureConsumers.add(consumerId);
                continue;
            }
            if (usedVersion < minReceivedMetadataVersion) {
                minReceivedMetadataVersion = usedVersion;
            }

            // create the new client metadata if necessary
            ClientMetadata clientMetadata = clientMetadataMap.get(info.processId());

            if (clientMetadata == null) {
                clientMetadata = new ClientMetadata(info.userEndPoint());
                clientMetadataMap.put(info.processId(), clientMetadata);
            }

            // add the consumer to the client
            clientMetadata.addConsumer(consumerId, info, subscription.ownedPartitions());
            allOwnedPartitions.addAll(subscription.ownedPartitions());
        }

        final boolean versionProbing;
        if (futureMetadataVersion == UNKNOWN) {
            versionProbing = false;
        } else {
            if (minReceivedMetadataVersion >= EARLIEST_PROBEABLE_VERSION) {
                log.info("Received a future (version probing) subscription (version: {})."
                             + " Sending assignment back (with supported version {}).",
                         futureMetadataVersion,
                         LATEST_SUPPORTED_VERSION);
                versionProbing = true;
            } else {
                throw new IllegalStateException(
                    "Received a future (version probing) subscription (version: " + futureMetadataVersion
                        + ") and an incompatible pre Kafka 2.0 subscription (version: " + minReceivedMetadataVersion
                        + ") at the same time."
                );
            }
        }

        if (minReceivedMetadataVersion < LATEST_SUPPORTED_VERSION) {
            log.info("Downgrading metadata to version {}. Latest supported version is {}.",
                     minReceivedMetadataVersion,
                     LATEST_SUPPORTED_VERSION);
        }

        log.debug("Constructed client metadata {} from the member subscriptions.", clientMetadataMap);

        // ---------------- Step Zero ---------------- //

        // parse the topology to determine the repartition source topics,
        // making sure they are created with the number of partitions as
        // the maximum of the depending sub-topologies source topics' number of partitions
        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = taskManager.builder().topicGroups();

        final Map<String, InternalTopicConfig> repartitionTopicMetadata = new HashMap<>();
        for (final InternalTopologyBuilder.TopicsInfo topicsInfo : topicGroups.values()) {
            for (final String topic : topicsInfo.sourceTopics) {
                if (!topicsInfo.repartitionSourceTopics.keySet().contains(topic) &&
                    !metadata.topics().contains(topic)) {
                    log.error("Missing source topic {} during assignment. Returning error {}.",
                              topic, AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.name());
                    return new GroupAssignment(
                        errorAssignment(clientMetadataMap, topic, AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code())
                    );
                }
            }
            for (final InternalTopicConfig topic : topicsInfo.repartitionSourceTopics.values()) {
                repartitionTopicMetadata.put(topic.name(), topic);
            }
        }

        boolean numPartitionsNeeded;
        do {
            numPartitionsNeeded = false;

            for (final InternalTopologyBuilder.TopicsInfo topicsInfo : topicGroups.values()) {
                for (final String topicName : topicsInfo.repartitionSourceTopics.keySet()) {
                    final Optional<Integer> maybeNumPartitions = repartitionTopicMetadata.get(topicName).numberOfPartitions();
                    Integer numPartitions = null;

                    if (!maybeNumPartitions.isPresent()) {
                        // try set the number of partitions for this repartition topic if it is not set yet
                        for (final InternalTopologyBuilder.TopicsInfo otherTopicsInfo : topicGroups.values()) {
                            final Set<String> otherSinkTopics = otherTopicsInfo.sinkTopics;

                            if (otherSinkTopics.contains(topicName)) {
                                // if this topic is one of the sink topics of this topology,
                                // use the maximum of all its source topic partitions as the number of partitions
                                for (final String sourceTopicName : otherTopicsInfo.sourceTopics) {
                                    final int numPartitionsCandidate;
                                    // It is possible the sourceTopic is another internal topic, i.e,
                                    // map().join().join(map())
                                    if (repartitionTopicMetadata.containsKey(sourceTopicName)
                                        && repartitionTopicMetadata.get(sourceTopicName).numberOfPartitions().isPresent()) {
                                        numPartitionsCandidate = repartitionTopicMetadata.get(sourceTopicName).numberOfPartitions().get();
                                    } else {
                                        final Integer count = metadata.partitionCountForTopic(sourceTopicName);
                                        if (count == null) {
                                            throw new IllegalStateException(
                                                "No partition count found for source topic "
                                                    + sourceTopicName
                                                    + ", but it should have been."
                                            );
                                        }
                                        numPartitionsCandidate = count;
                                    }

                                    if (numPartitions == null || numPartitionsCandidate > numPartitions) {
                                        numPartitions = numPartitionsCandidate;
                                    }
                                }
                            }
                        }
                        // if we still have not find the right number of partitions,
                        // another iteration is needed
                        if (numPartitions == null) {
                            numPartitionsNeeded = true;
                        } else {
                            repartitionTopicMetadata.get(topicName).setNumberOfPartitions(numPartitions);
                        }
                    }
                }
            }
        } while (numPartitionsNeeded);


        // ensure the co-partitioning topics within the group have the same number of partitions,
        // and enforce the number of partitions for those repartition topics to be the same if they
        // are co-partitioned as well.
        ensureCopartitioning(taskManager.builder().copartitionGroups(), repartitionTopicMetadata, metadata);

        // make sure the repartition source topics exist with the right number of partitions,
        // create these topics if necessary
        prepareTopic(repartitionTopicMetadata);

        // augment the metadata with the newly computed number of partitions for all the
        // repartition source topics
        final Map<TopicPartition, PartitionInfo> allRepartitionTopicPartitions = new HashMap<>();
        for (final Map.Entry<String, InternalTopicConfig> entry : repartitionTopicMetadata.entrySet()) {
            final String topic = entry.getKey();
            final int numPartitions = entry.getValue().numberOfPartitions().orElse(-1);

            for (int partition = 0; partition < numPartitions; partition++) {
                allRepartitionTopicPartitions.put(
                    new TopicPartition(topic, partition),
                    new PartitionInfo(topic, partition, null, new Node[0], new Node[0])
                );
            }
        }

        final Cluster fullMetadata = metadata.withPartitions(allRepartitionTopicPartitions);
        taskManager.setClusterMetadata(fullMetadata);

        log.debug("Created repartition topics {} from the parsed topology.", allRepartitionTopicPartitions.values());

        // ---------------- Step One ---------------- //

        // get the tasks as partition groups from the partition grouper
        final Set<String> allSourceTopics = new HashSet<>();
        final Map<Integer, Set<String>> sourceTopicsByGroup = new HashMap<>();
        for (final Map.Entry<Integer, InternalTopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            allSourceTopics.addAll(entry.getValue().sourceTopics);
            sourceTopicsByGroup.put(entry.getKey(), entry.getValue().sourceTopics);
        }

        final Map<TaskId, Set<TopicPartition>> partitionsForTask =
            partitionGrouper.partitionGroups(sourceTopicsByGroup, fullMetadata);

        final Map<TopicPartition, TaskId> taskForPartition = new HashMap<>();

        // check if all partitions are assigned, and there are no duplicates of partitions in multiple tasks
        final Set<TopicPartition> allAssignedPartitions = new HashSet<>();
        final Map<Integer, Set<TaskId>> tasksByTopicGroup = new HashMap<>();
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : partitionsForTask.entrySet()) {
            final TaskId id = entry.getKey();
            final Set<TopicPartition> partitions = entry.getValue();

            for (final TopicPartition partition : partitions) {
                taskForPartition.put(partition, id);
                if (allAssignedPartitions.contains(partition)) {
                    log.warn("Partition {} is assigned to more than one tasks: {}", partition, partitionsForTask);
                }
            }
            allAssignedPartitions.addAll(partitions);

            tasksByTopicGroup.computeIfAbsent(id.topicGroupId, k -> new HashSet<>()).add(id);
        }
        for (final String topic : allSourceTopics) {
            final List<PartitionInfo> partitionInfoList = fullMetadata.partitionsForTopic(topic);
            if (partitionInfoList.isEmpty()) {
                log.warn("No partitions found for topic {}", topic);
            } else {
                for (final PartitionInfo partitionInfo : partitionInfoList) {
                    final TopicPartition partition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                    if (!allAssignedPartitions.contains(partition)) {
                        log.warn("Partition {} is not assigned to any tasks: {}"
                                     + " Possible causes of a partition not getting assigned"
                                     + " is that another topic defined in the topology has not been"
                                     + " created when starting your streams application,"
                                     + " resulting in no tasks created for this topology at all.", partition, partitionsForTask);
                    }
                }
            }
        }

        // add tasks to state change log topic subscribers
        final Map<String, InternalTopicConfig> changelogTopicMetadata = new HashMap<>();
        for (final Map.Entry<Integer, InternalTopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            final int topicGroupId = entry.getKey();
            final Map<String, InternalTopicConfig> stateChangelogTopics = entry.getValue().stateChangelogTopics;

            for (final InternalTopicConfig topicConfig : stateChangelogTopics.values()) {
                // the expected number of partitions is the max value of TaskId.partition + 1
                int numPartitions = UNKNOWN;
                if (tasksByTopicGroup.get(topicGroupId) != null) {
                    for (final TaskId task : tasksByTopicGroup.get(topicGroupId)) {
                        if (numPartitions < task.partition + 1) {
                            numPartitions = task.partition + 1;
                        }
                    }
                    topicConfig.setNumberOfPartitions(numPartitions);

                    changelogTopicMetadata.put(topicConfig.name(), topicConfig);
                } else {
                    log.debug("No tasks found for topic group {}", topicGroupId);
                }
            }
        }

        prepareTopic(changelogTopicMetadata);

        log.debug("Created state changelog topics {} from the parsed topology.", changelogTopicMetadata.values());

        // ---------------- Step Two ---------------- //

        final Map<UUID, ClientState> states = new HashMap<>();
        for (final Map.Entry<UUID, ClientMetadata> entry : clientMetadataMap.entrySet()) {
            final ClientState state = entry.getValue().state;
            states.put(entry.getKey(), state);

            // Either the active tasks (eager) OR the owned partitions (cooperative) were encoded in the subscription
            // according to the rebalancing protocol, so convert any partitions in a client to tasks where necessary
            if (!state.ownedPartitions().isEmpty()) {
                for (final Map.Entry<TopicPartition, String> partitionEntry : state.ownedPartitions().entrySet()) {
                    final TopicPartition tp = partitionEntry.getKey();
                    final TaskId task = taskForPartition.get(tp);
                    if (task != null) {
                        state.addPreviousActiveTask(task);
                    } else {
                        log.error("No task found for topic partition {}", tp);
                    }
                }
            }
        }

        log.debug("Assigning tasks {} to clients {} with number of replicas {}",
                  partitionsForTask.keySet(), states, numStandbyReplicas);

        // assign tasks to clients
        final StickyTaskAssignor<UUID> taskAssignor = new StickyTaskAssignor<>(states, partitionsForTask.keySet());
        taskAssignor.assign(numStandbyReplicas);

        log.info("Assigned tasks to clients as {}{}.", Utils.NL, states.entrySet().stream()
            .map(Map.Entry::toString).collect(Collectors.joining(Utils.NL)));

        // ---------------- Step Three ---------------- //

        // construct the global partition assignment per host map
        final Map<HostInfo, Set<TopicPartition>> partitionsByHostState = new HashMap<>();
        if (minReceivedMetadataVersion >= 2) {
            for (final Map.Entry<UUID, ClientMetadata> entry : clientMetadataMap.entrySet()) {
                final HostInfo hostInfo = entry.getValue().hostInfo;

                // if application server is configured, also include host state map
                if (hostInfo != null) {
                    final Set<TopicPartition> topicPartitions = new HashSet<>();
                    final ClientState state = entry.getValue().state;

                    for (final TaskId id : state.activeTasks()) {
                        topicPartitions.addAll(partitionsForTask.get(id));
                    }

                    partitionsByHostState.put(hostInfo, topicPartitions);
                }
            }
        }
        taskManager.setPartitionsByHostState(partitionsByHostState);

        final Map<String, Assignment> assignment;
        if (versionProbing) {
            assignment = versionProbingAssignment(
                clientMetadataMap,
                partitionsForTask,
                partitionsByHostState,
                futureConsumers,
                allOwnedPartitions,
                minReceivedMetadataVersion
            );
        } else {
            assignment = computeNewAssignment(
                clientMetadataMap,
                partitionsForTask,
                partitionsByHostState,
                allOwnedPartitions,
                minReceivedMetadataVersion
            );
        }

        return new GroupAssignment(assignment);
    }

    private Map<String, Assignment> computeNewAssignment(final Map<UUID, ClientMetadata> clientsMetadata,
                                                         final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                                         final Map<HostInfo, Set<TopicPartition>> partitionsByHostState,
                                                         final Set<TopicPartition> allOwnedPartitions,
                                                         final int minUserMetadataVersion) {
        final Map<String, Assignment> assignment = new HashMap<>();
        rebalanceRequired = false;

        // within the client, distribute tasks to its owned consumers
        for (final ClientMetadata clientMetadata : clientsMetadata.values()) {
            final ClientState state = clientMetadata.state;

            // Try to avoid triggering another rebalance by giving active tasks back to their previous owners within a
            // task, without violating load balance. If we already know another rebalance will be required, or the
            // client had no owned partitions, try to balance the workload as evenly as possible by interleaving the
            // tasks among consumers and hopefully spreading the heavier subtopologies evenly across threads.
            final Map<String, List<TaskId>> activeTaskAssignments =
                rebalanceRequired || state.ownedPartitions().isEmpty() ?
                interleaveTasksByGroupId(state.activeTasks(), clientMetadata.consumers) :
                giveTasksBackToConsumers(clientMetadata, partitionsForTask, allOwnedPartitions);

            addClientAssignments(
                assignment,
                clientMetadata,
                partitionsForTask,
                partitionsByHostState,
                allOwnedPartitions,
                activeTaskAssignments,
                Collections.emptySet(),
                minUserMetadataVersion);
        }

        return assignment;
    }

    private Map<String, Assignment> versionProbingAssignment(final Map<UUID, ClientMetadata> clientsMetadata,
                                                             final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                                             final Map<HostInfo, Set<TopicPartition>> partitionsByHostState,
                                                             final Set<String> futureConsumers,
                                                             final Set<TopicPartition> allOwnedPartitions,
                                                             final int minUserMetadataVersion) {
        final Map<String, Assignment> assignment = new HashMap<>();

        // Since we know another rebalance will be triggered anyway, just try and generate a balanced assignment
        // (without violating cooperative protocol) now so that on the second rebalance we can just give tasks
        // back to their previous owners
        // within the client, distribute tasks to its owned consumers
        for (final ClientMetadata clientMetadata : clientsMetadata.values()) {
            final ClientState state = clientMetadata.state;

            final Map<String, List<TaskId>> interleavedActive =
                interleaveTasksByGroupId(state.activeTasks(), clientMetadata.consumers);

            addClientAssignments(
                assignment,
                clientMetadata,
                partitionsForTask,
                partitionsByHostState,
                allOwnedPartitions,
                interleavedActive,
                futureConsumers,
                minUserMetadataVersion);
        }

        return assignment;
    }

    private void addClientAssignments(final Map<String, Assignment> assignment,
                                      final ClientMetadata clientMetadata,
                                      final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                      final Map<HostInfo, Set<TopicPartition>> partitionsByHostState,
                                      final Set<TopicPartition> allOwnedPartitions,
                                      final Map<String, List<TaskId>> activeTaskAssignments,
                                      final Set<String> futureConsumers,
                                      final int minUserMetadataVersion) {
        final ClientState clientState = clientMetadata.state;
        final Map<String, List<TaskId>> interleavedStandby =
            interleaveTasksByGroupId(clientState.standbyTasks(), clientMetadata.consumers);

        for (final String consumer : clientMetadata.consumers) {
            final Map<TaskId, Set<TopicPartition>> standby = new HashMap<>();
            final List<AssignedPartition> assignedPartitions = new ArrayList<>();

            final List<TaskId> assignedActiveList = activeTaskAssignments.get(consumer);

            for (final TaskId taskId : assignedActiveList) {
                for (final TopicPartition partition : partitionsForTask.get(taskId)) {
                    final String oldOwner = clientState.ownedPartitions().get(partition);
                    final boolean newPartitionForConsumer = oldOwner == null || !oldOwner.equals(consumer);

                    // If the partition is new to this consumer but is still owned by another, remove from the assignment
                    // until it has been revoked and can safely be reassigned according the COOPERATIVE protocol
                    if (newPartitionForConsumer && allOwnedPartitions.contains(partition)) {
                        log.debug("Removing task {} from assignment until it is safely revoked", taskId);
                        break;
                    } else {
                        assignedPartitions.add(new AssignedPartition(taskId, partition));
                    }
                }
            }

            if (!clientState.standbyTasks().isEmpty()) {
                final List<TaskId> assignedStandbyList = interleavedStandby.get(consumer);
                for (final TaskId taskId : assignedStandbyList) {
                    standby.computeIfAbsent(taskId, k -> new HashSet<>()).addAll(partitionsForTask.get(taskId));
                }
            }

            Collections.sort(assignedPartitions);
            final List<TaskId> active = new ArrayList<>();
            final List<TopicPartition> activePartitions = new ArrayList<>();
            for (final AssignedPartition partition : assignedPartitions) {
                active.add(partition.taskId);
                activePartitions.add(partition.partition);
            }

            // If this is a version probing rebalance, encode the assignment using the leader's latest
            // supported version for "future" consumers who sent a higher version subscription
            if (futureConsumers.contains(consumer)) {
                assignment.put(
                    consumer,
                    new Assignment(
                        activePartitions,
                        new AssignmentInfo(
                            LATEST_SUPPORTED_VERSION,
                            active,
                            standby,
                            partitionsByHostState,
                            0
                        ).encode()
                    )
                );
            } else {
                assignment.put(
                    consumer,
                    new Assignment(
                        activePartitions,
                        new AssignmentInfo(
                            minUserMetadataVersion,
                            active,
                            standby,
                            partitionsByHostState,
                            0
                        ).encode()
                    )
                );
            }
        }
    }

    /**
     * Generates an assignment that tries to satisfy two conditions: no active task previously owned by a consumer
     * be assigned to another, and the number of tasks is evenly distributed throughout the client.
     * <p>
     * If it is impossible to satisfy both constraints we abort and return the interleaved assignment instead.
     *
     * @param metadata metadata for this client
     * @param partitionsForTask mapping from task to its associated partitions
     * @param allOwnedPartitions set of all partitions claimed as owned by the group
     * @return task assignment for the consumers of this client
     */
    Map<String, List<TaskId>> giveTasksBackToConsumers(final ClientMetadata metadata,
                                                       final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                                       final Set<TopicPartition> allOwnedPartitions) {
        final Map<String, List<TaskId>> assignments = new HashMap<>();
        final LinkedList<TaskId> newTasks = new LinkedList<>();
        final ClientState state = metadata.state;
        final Set<String> consumers = metadata.consumers;
        final Set<String> unfilledConsumers = new HashSet<>(consumers);

        final int maxTasksPerClient = (int) Math.ceil(((double) state.activeTaskCount()) / consumers.size());

        for (final TaskId task : state.activeTasks()) {
            final String consumer = previousConsumerOfTaskPartitions(partitionsForTask.get(task), state.ownedPartitions(), allOwnedPartitions);

            // if this task's partitions were not all owned by the same consumer in this client, abort
            if (consumer == null) {
                rebalanceRequired = true;
                return interleaveTasksByGroupId(state.activeTasks(), consumers);
            }

            // if this is a new task, or its old consumer no longer exists, it can be freely assigned
            if (consumer.equals("")) {
                newTasks.add(task);
            } else {
                assignments.computeIfAbsent(consumer, c -> new ArrayList<>());

                // if this task was owned by a consumer that is at capacity, abort
                if (assignments.get(consumer).size() >= maxTasksPerClient) {
                    rebalanceRequired = true;
                    return interleaveTasksByGroupId(state.activeTasks(), consumers);
                } else {
                    assignments.get(consumer).add(task);
                    // if we have now reach capacity, remove from set of consumers who need more tasks
                    if (assignments.get(consumer).size() == maxTasksPerClient) {
                        unfilledConsumers.remove(consumer);
                    }
                }
            }
        }

        // initialize task list for any consumers not yet in the assignment
        for (final String consumer : unfilledConsumers) {
            assignments.computeIfAbsent(consumer, c -> new ArrayList<>());
        }

        // interleave any remaining tasks among the remaining consumers with capacity
        Collections.sort(newTasks);
        while (!newTasks.isEmpty()) {
            if (unfilledConsumers.isEmpty()) {
                throw new IllegalStateException("Some tasks could not be distributed");
            }

            final Iterator<String> consumerIt = unfilledConsumers.iterator();
            while (consumerIt.hasNext()) {
                final String consumer = consumerIt.next();
                final List<TaskId> consumerAssignment = assignments.get(consumer);
                final TaskId task = newTasks.poll();
                if (task == null) {
                    break;
                }

                consumerAssignment.add(task);
                if (consumerAssignment.size() == maxTasksPerClient) {
                    consumerIt.remove();
                }
            }
        }

        return assignments;
    }

    /**
     * Get the previous consumer for the partitions of a task
     *
     * @param taskPartitions the TopicPartitions for a single given task
     * @param clientOwnedPartitions the partitions owned by all consumers in a client
     * @param allOwnedPartitions all partitions claimed as owned by any consumer in any client
     * @return the previous consumer that owned these partitions
     *         empty string if it is a new task, or its owner is no longer in the group
     *         null if its previous owner is from a different client or the task's partitions had different owners
     */
    String previousConsumerOfTaskPartitions(final Set<TopicPartition> taskPartitions,
                                           final Map<TopicPartition, String> clientOwnedPartitions,
                                           final Set<TopicPartition> allOwnedPartitions) {
        String firstConsumer = "";
        for (final TopicPartition tp : taskPartitions) {
            // initialize the first consumer if this is the first iteration
            if (firstConsumer != null && firstConsumer.equals("")) {
                firstConsumer = clientOwnedPartitions.get(tp);
            }

            final String thisConsumer = clientOwnedPartitions.get(tp);

            // if no one in the client had this partition but someone claims it as owned, abort and return null
            if (thisConsumer == null && allOwnedPartitions.contains(tp)) {
                log.debug("Client was assigned partition previously owned by another client, will trigger another rebalance.");
                return null;
            }

            // if the partitions of this task claim two different previous owners, abort and return null
            if (thisConsumer != null && !thisConsumer.equals(firstConsumer)) {
                log.error("Mapping from task to partitions has changed!");
                return null;
            }
        }
        // if we haven't found a previous owner, this is either a new partition or its old owner left the group
        if (firstConsumer == null) {
            return "";
        }
        return firstConsumer;
    }

    // visible for testing
    static Map<String, List<TaskId>> interleaveTasksByGroupId(final Collection<TaskId> taskIds, final Set<String> consumers) {
        final LinkedList<TaskId> sortedTasks = new LinkedList<>(taskIds);
        Collections.sort(sortedTasks);
        final Map<String, List<TaskId>> taskIdsForConsumerAssignment = new TreeMap<>();
        for (final String consumer : consumers) {
            taskIdsForConsumerAssignment.put(consumer, new ArrayList<>());
        }
        while (!sortedTasks.isEmpty()) {
            for (final Map.Entry<String, List<TaskId>> consumerTaskIds : taskIdsForConsumerAssignment.entrySet()) {
                final List<TaskId> taskIdList = consumerTaskIds.getValue();
                final TaskId taskId = sortedTasks.poll();
                if (taskId == null) {
                    break;
                }
                taskIdList.add(taskId);
            }
        }
        return taskIdsForConsumerAssignment;
    }

    private void upgradeSubscriptionVersionIfNeeded(final int leaderSupportedVersion) {
        if (leaderSupportedVersion > usedSubscriptionMetadataVersion) {
            log.info("Sent a version {} subscription and group leader's latest supported version is {}. " +
                         "Upgrading subscription metadata version to {} for next rebalance.",
                     usedSubscriptionMetadataVersion,
                     leaderSupportedVersion,
                     leaderSupportedVersion);
            usedSubscriptionMetadataVersion = leaderSupportedVersion;
        }
    }

    /**
     * @throws TaskAssignmentException if there is no task id for one of the partitions specified
     */
    @Override
    public void onAssignment(final Assignment assignment, final ConsumerGroupMetadata metadata) {
        final List<TopicPartition> partitions = new ArrayList<>(assignment.partitions());
        partitions.sort(PARTITION_COMPARATOR);

        final AssignmentInfo info = AssignmentInfo.decode(assignment.userData());
        if (info.errCode() != AssignorError.NONE.code()) {
            // set flag to shutdown streams app
            assignmentErrorCode.set(info.errCode());
            return;
        }
        final int receivedAssignmentMetadataVersion = info.version();
        final int leaderSupportedVersion = info.latestSupportedVersion();

        if (receivedAssignmentMetadataVersion > usedSubscriptionMetadataVersion) {
            throw new IllegalStateException(
                "Sent a version " + usedSubscriptionMetadataVersion
                    + " subscription but got an assignment with higher version "
                    + receivedAssignmentMetadataVersion + "."
            );
        }

        // check for version probing
        if (receivedAssignmentMetadataVersion < usedSubscriptionMetadataVersion
            && receivedAssignmentMetadataVersion >= EARLIEST_PROBEABLE_VERSION) {

            // if received version was the same as the leader's, we triggered the version probing and must downgrade
            if (receivedAssignmentMetadataVersion == leaderSupportedVersion) {
                log.info(
                    "Sent a version {} subscription and got version {} assignment back (successful version probing). " +
                        "Downgrading subscription metadata to received version and trigger new rebalance.",
                    usedSubscriptionMetadataVersion,
                    receivedAssignmentMetadataVersion
                );
            } else {
                log.info(
                    "Sent a version {} subscription and got version {} assignment back (successful version probing). " +
                        "Setting subscription metadata to leaders supported version {} and trigger new rebalance.",
                    usedSubscriptionMetadataVersion,
                    receivedAssignmentMetadataVersion,
                    leaderSupportedVersion
                );
            }
            usedSubscriptionMetadataVersion = leaderSupportedVersion;
            assignmentErrorCode.set(AssignorError.VERSION_PROBING.code());
            return;
        }

        // version 1 field
        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        // version 2 fields
        final Map<TopicPartition, PartitionInfo> topicToPartitionInfo = new HashMap<>();
        final Map<HostInfo, Set<TopicPartition>> partitionsByHost;

        final Map<TopicPartition, TaskId> partitionsToTaskId = new HashMap<>();

        switch (receivedAssignmentMetadataVersion) {
            case VERSION_ONE:
                processVersionOneAssignment(logPrefix, info, partitions, activeTasks, partitionsToTaskId);
                partitionsByHost = Collections.emptyMap();
                break;
            case VERSION_TWO:
                processVersionTwoAssignment(logPrefix, info, partitions, activeTasks, topicToPartitionInfo, partitionsToTaskId);
                partitionsByHost = info.partitionsByHost();
                break;
            case VERSION_THREE:
            case VERSION_FOUR:
            case VERSION_FIVE:
                upgradeSubscriptionVersionIfNeeded(leaderSupportedVersion);
                processVersionTwoAssignment(logPrefix, info, partitions, activeTasks, topicToPartitionInfo, partitionsToTaskId);
                partitionsByHost = info.partitionsByHost();
                break;
            default:
                throw new IllegalStateException(
                    "This code should never be reached."
                        + " Please file a bug report at https://issues.apache.org/jira/projects/KAFKA/"
                );
        }

        taskManager.setClusterMetadata(Cluster.empty().withPartitions(topicToPartitionInfo));
        taskManager.setPartitionsByHostState(partitionsByHost);
        taskManager.setPartitionsToTaskId(partitionsToTaskId);
        taskManager.setAssignmentMetadata(activeTasks, info.standbyTasks());
        taskManager.updateSubscriptionsFromAssignment(partitions);
        taskManager.setRebalanceInProgress(false);
    }

    private static void processVersionOneAssignment(final String logPrefix,
                                                    final AssignmentInfo info,
                                                    final List<TopicPartition> partitions,
                                                    final Map<TaskId, Set<TopicPartition>> activeTasks,
                                                    final Map<TopicPartition, TaskId> partitionsToTaskId) {
        // the number of assigned partitions should be the same as number of active tasks, which
        // could be duplicated if one task has more than one assigned partitions
        if (partitions.size() != info.activeTasks().size()) {
            throw new TaskAssignmentException(
                String.format(
                    "%sNumber of assigned partitions %d is not equal to "
                        + "the number of active taskIds %d, assignmentInfo=%s",
                    logPrefix, partitions.size(),
                    info.activeTasks().size(), info.toString()
                )
            );
        }

        for (int i = 0; i < partitions.size(); i++) {
            final TopicPartition partition = partitions.get(i);
            final TaskId id = info.activeTasks().get(i);
            activeTasks.computeIfAbsent(id, k -> new HashSet<>()).add(partition);
            partitionsToTaskId.put(partition, id);
        }
    }

    public static void processVersionTwoAssignment(final String logPrefix,
                                                   final AssignmentInfo info,
                                                   final List<TopicPartition> partitions,
                                                   final Map<TaskId, Set<TopicPartition>> activeTasks,
                                                   final Map<TopicPartition, PartitionInfo> topicToPartitionInfo,
                                                   final Map<TopicPartition, TaskId> partitionsToTaskId) {
        processVersionOneAssignment(logPrefix, info, partitions, activeTasks, partitionsToTaskId);

        // process partitions by host
        final Map<HostInfo, Set<TopicPartition>> partitionsByHost = info.partitionsByHost();
        for (final Set<TopicPartition> value : partitionsByHost.values()) {
            for (final TopicPartition topicPartition : value) {
                topicToPartitionInfo.put(
                    topicPartition,
                    new PartitionInfo(
                        topicPartition.topic(),
                        topicPartition.partition(),
                        null,
                        new Node[0],
                        new Node[0]
                    )
                );
            }
        }
    }

    /**
     * Internal helper function that creates a Kafka topic
     *
     * @param topicPartitions Map that contains the topic names to be created with the number of partitions
     */
    private void prepareTopic(final Map<String, InternalTopicConfig> topicPartitions) {
        log.debug("Starting to validate internal topics {} in partition assignor.", topicPartitions);

        // first construct the topics to make ready
        final Map<String, InternalTopicConfig> topicsToMakeReady = new HashMap<>();

        for (final InternalTopicConfig topic : topicPartitions.values()) {
            final Optional<Integer> numPartitions = topic.numberOfPartitions();
            if (!numPartitions.isPresent()) {
                throw new StreamsException(
                    String.format("%sTopic [%s] number of partitions not defined",
                                  logPrefix, topic.name())
                );
            }

            topic.setNumberOfPartitions(numPartitions.get());
            topicsToMakeReady.put(topic.name(), topic);
        }

        if (!topicsToMakeReady.isEmpty()) {
            internalTopicManager.makeReady(topicsToMakeReady);
        }

        log.debug("Completed validating internal topics {} in partition assignor.", topicPartitions);
    }

    private void ensureCopartitioning(final Collection<Set<String>> copartitionGroups,
                                      final Map<String, InternalTopicConfig> allRepartitionTopicsNumPartitions,
                                      final Cluster metadata) {
        for (final Set<String> copartitionGroup : copartitionGroups) {
            copartitionedTopicsEnforcer.enforce(copartitionGroup, allRepartitionTopicsNumPartitions, metadata);
        }
    }


    // following functions are for test only
    void setRebalanceProtocol(final RebalanceProtocol rebalanceProtocol) {
        this.rebalanceProtocol = rebalanceProtocol;
    }

    void setInternalTopicManager(final InternalTopicManager internalTopicManager) {
        this.internalTopicManager = internalTopicManager;
    }

}

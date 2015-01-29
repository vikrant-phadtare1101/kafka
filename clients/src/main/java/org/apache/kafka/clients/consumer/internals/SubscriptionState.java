package org.apache.kafka.clients.consumer.internals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer
 */
public class SubscriptionState {

    /* the list of topics the user has requested */
    private final Set<String> subscribedTopics;

    /* the list of partitions the user has requested */
    private final Set<TopicPartition> subscribedPartitions;

    /* the list of partitions currently assigned */
    private final Set<TopicPartition> assignedPartitions;

    /* the offset exposed to the user */
    private final Map<TopicPartition, Long> consumed;

    /* the current point we have fetched up to */
    private final Map<TopicPartition, Long> fetched;

    /* the last committed offset for each partition */
    private final Map<TopicPartition, Long> committed;

    /* do we need to request a partition assignment from the co-ordinator? */
    private boolean needsPartitionAssignment;

    public SubscriptionState() {
        this.subscribedTopics = new HashSet<String>();
        this.subscribedPartitions = new HashSet<TopicPartition>();
        this.assignedPartitions = new HashSet<TopicPartition>();
        this.consumed = new HashMap<TopicPartition, Long>();
        this.fetched = new HashMap<TopicPartition, Long>();
        this.committed = new HashMap<TopicPartition, Long>();
        this.needsPartitionAssignment = false;
    }

    public void subscribe(String topic) {
        if (this.subscribedPartitions.size() > 0)
            throw new IllegalStateException("Subcription to topics and partitions are mutually exclusive");
        if (!this.subscribedTopics.contains(topic)) {
            this.subscribedTopics.add(topic);
            this.needsPartitionAssignment = true;
        }
    }

    public void unsubscribe(String topic) {
        if (!this.subscribedTopics.contains(topic))
            throw new IllegalStateException("Topic " + topic + " was never subscribed to.");
        this.subscribedTopics.remove(topic);
        this.needsPartitionAssignment = true;
        for(TopicPartition tp: assignedPartitions())
            if(topic.equals(tp.topic()))
                clearPartition(tp);
    }

    public void subscribe(TopicPartition tp) {
        if (this.subscribedTopics.size() > 0)
            throw new IllegalStateException("Subcription to topics and partitions are mutually exclusive");
        this.subscribedPartitions.add(tp);
        this.assignedPartitions.add(tp);
    }

    public void unsubscribe(TopicPartition partition) {
        if (!subscribedPartitions.contains(partition))
            throw new IllegalStateException("Partition " + partition + " was never subscribed to.");
        subscribedPartitions.remove(partition);
        clearPartition(partition);
    }
    
    private void clearPartition(TopicPartition tp) {
        this.assignedPartitions.remove(tp);
        this.committed.remove(tp);
        this.fetched.remove(tp);
        this.consumed.remove(tp);
    }

    public void clearAssignment() {
        this.assignedPartitions.clear();
        this.committed.clear();
        this.fetched.clear();
        this.needsPartitionAssignment = !subscribedTopics().isEmpty();
    }

    public Set<String> subscribedTopics() {
        return this.subscribedTopics;
    }

    public Long fetched(TopicPartition tp) {
        return this.fetched.get(tp);
    }

    public void fetched(TopicPartition tp, long offset) {
        if (!this.assignedPartitions.contains(tp))
            throw new IllegalArgumentException("Can't change the fetch position for a partition you are not currently subscribed to.");
        this.fetched.put(tp, offset);
    }

    public void committed(TopicPartition tp, long offset) {
        this.committed.put(tp, offset);
    }

    public Long committed(TopicPartition tp) {
        return this.committed.get(tp);
    }
    
    public void seek(TopicPartition tp, long offset) {
        fetched(tp, offset);
        consumed(tp, offset);
    }

    public Set<TopicPartition> assignedPartitions() {
        return this.assignedPartitions;
    }

    public boolean partitionsAutoAssigned() {
        return !this.subscribedTopics.isEmpty();
    }

    public void consumed(TopicPartition tp, long offset) {
        if (!this.assignedPartitions.contains(tp))
            throw new IllegalArgumentException("Can't change the consumed position for a partition you are not currently subscribed to.");
        this.consumed.put(tp, offset);
    }

    public Long consumed(TopicPartition partition) {
        return this.consumed.get(partition);
    }

    public Map<TopicPartition, Long> allConsumed() {
        return this.consumed;
    }

    public boolean hasAllFetchPositions() {
        return this.fetched.size() >= this.assignedPartitions.size();
    }

    public Set<TopicPartition> missingFetchPositions() {
        Set<TopicPartition> copy = new HashSet<TopicPartition>(this.assignedPartitions);
        copy.removeAll(this.fetched.keySet());
        return copy;
    }

    public boolean needsPartitionAssignment() {
        return this.needsPartitionAssignment;
    }

    public void changePartitionAssignment(List<TopicPartition> assignments) {
        for (TopicPartition tp : assignments)
            if (!this.subscribedTopics.contains(tp.topic()))
                throw new IllegalArgumentException("Assigned partition " + tp + " for non-subscribed topic.");
        this.clearAssignment();
        this.assignedPartitions.addAll(assignments);
        this.needsPartitionAssignment = false;
    }

}
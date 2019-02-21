package com.kafka.partitioner;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.Cluster;

import java.util.*;

public class ItemPartitioner implements Partitioner {

    private final Set<String> popularItems;
    public ItemPartitioner() {
        popularItems = new HashSet<>();
    }

    @Override
    public int partition(final String topic,
                         final Object objectKey,
                         final byte[] keyBytes,
                         final Object value,
                         final byte[] valueBytes,
                         final Cluster cluster) {

        final List<PartitionInfo> partitionInfoList =
                cluster.availablePartitionsForTopic(topic);
        final int partitionCount = partitionInfoList.size();
        final int popularPartition = partitionCount -1;
        final int normalPartitionCount = partitionCount -1;

        final String key = ((String) objectKey);

        if (popularItems.contains(key)) {
            return popularPartition;
        } else {
            return Math.abs(key.hashCode()) % normalPartitionCount;
        }

    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final String popularItemsStr = (String) configs.get("popularItems");
        Arrays.stream(popularItemsStr.split(","))
                .forEach(popularItems::add);
    }

}

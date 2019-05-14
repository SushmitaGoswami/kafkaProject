package com.kafka.consumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class ItemConsumerRebalanceListener implements ConsumerRebalanceListener{
	
	private KafkaConsumer<?, ?> consumer;
	private Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<TopicPartition, OffsetAndMetadata>(); 
	
	public  ItemConsumerRebalanceListener(KafkaConsumer<?, ?> consumer) {
		this.consumer = consumer;
	}
	
	public void addOffset(String topic, int partition, long offset) {
		currentOffset.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset));
		
	}
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    	System.out.println("Following Partition Revoked...");
        for(TopicPartition partition : partitions) {
     	   System.out.println("Topic Name  " + partition.topic() + "  ,  Parttion  " + partition.partition());
        }
        
        System.out.println("Following Partition will be committed...");
        for(TopicPartition partition : currentOffset.keySet()) {
        	System.out.println("Topic Name  " + partition.topic() + "  ,  Parttion  " + partition.partition());
        }
        
        consumer.commitSync(currentOffset);
        consumer.close();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    	System.out.println("Following Partition Assigned...");
        for(TopicPartition partition : partitions) {
     	   System.out.println("Topic Name  " + partition.topic() + "  ,  Parttion  " + partition.partition());
        }
    }

}

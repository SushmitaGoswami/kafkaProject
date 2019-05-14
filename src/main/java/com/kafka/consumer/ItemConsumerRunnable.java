package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.kafka.model.Item;

public class ItemConsumerRunnable<K,V extends Item> implements Runnable{

	private ItemConsumer consumer;
	public ItemConsumerRunnable(ItemConsumer consumer) {
		super();
		this.consumer = consumer;
	}

	
	@Override
	public void run() {
		try {
			while(true) {
				ConsumerRecords<String, Item> consumerRecords = consumer.poll(100);
				for(ConsumerRecord<String, Item> consumerRecord : consumerRecords) {
					consumer.getListener().addOffset(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
			}
		}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		finally {
			consumer.close();
		}
	}
	
}

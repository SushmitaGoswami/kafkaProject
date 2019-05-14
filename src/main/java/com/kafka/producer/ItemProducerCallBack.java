package com.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ItemProducerCallBack implements Callback {

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if(exception != null) {
			System.out.println("Error in sending to topic " + metadata.topic() + " and partition " + metadata.partition());
		}
	}
	
}

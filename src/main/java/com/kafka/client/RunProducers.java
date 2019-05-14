package com.kafka.client;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Producer;

import com.kafka.model.Item;
import com.kafka.producer.ItemProducer;
import com.kafka.producer.ItemSenderRunnable;

public class RunProducers {
	public static void start() {
		Producer<String, Item> itemProducer = ItemProducer.createProducer();
		List<ItemSenderRunnable<String,Item>> itemSenders = getSenderList(itemProducer);
		final ExecutorService executorService = Executors.newFixedThreadPool(itemSenders.size());
		itemSenders.forEach(executorService::submit);
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executorService.shutdown();
            try {
                executorService.awaitTermination(200, TimeUnit.MILLISECONDS);
                itemProducer.flush();
                itemProducer.close(10_000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

	private static List<ItemSenderRunnable<String,Item>> getSenderList(final Producer<String, Item> producer){
		return Arrays.asList(
				new ItemSenderRunnable<String, Item>(producer, "orders", 
						Arrays.asList(new Item(1,"laptop",true), new Item(2,"mobile",false))),
				
				new ItemSenderRunnable<String, Item>(producer, "orders", 
						Arrays.asList(new Item(1,"t-shirt",false), new Item(2,"jeans",false)))
				
				);
	}
	
}

package com.kafka.client;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import com.kafka.consumer.ItemConsumer;
import com.kafka.consumer.ItemConsumerRunnable;
import com.kafka.model.Item;

public class RunConsumers {
	public static void start() {
		int noOfConsumers = 3;
		final ExecutorService executorService = Executors.newFixedThreadPool(noOfConsumers);
		IntStream.range(0, noOfConsumers).forEach(index -> {
			final ItemConsumerRunnable<String, Item> itemConsumerRunnable = new ItemConsumerRunnable<>
				(new ItemConsumer(Arrays.asList("retail")));
			executorService.submit(itemConsumerRunnable);
		});
	}

}

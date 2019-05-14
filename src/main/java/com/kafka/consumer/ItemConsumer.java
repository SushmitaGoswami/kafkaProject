package com.kafka.consumer;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.kafka.common.ItemDeserializer;
import com.kafka.config.ItemConsumerConfig;
import com.kafka.model.Item;

public class ItemConsumer{
	 protected final static String CONSUMER_PROPS = "consumerConfig.properties";
	    protected static Properties config;
	    private ItemConsumerRebalanceListener listener;
	    private KafkaConsumer<String, Item> consumer;
	    
	    public ItemConsumer(List<String> topics) {

	        final Properties props = new Properties();
	        loadConfigProperties();
	        setupBootstrapAndDeSerializers(props);
	        KafkaConsumer<String, Item> consumer = new KafkaConsumer<String, Item>(props);
	        this.listener = new ItemConsumerRebalanceListener(consumer);
	        consumer.subscribe(topics, this.listener);
	        this.consumer = consumer;
	    }

	    public ConsumerRecords<String, Item> poll(final long timeoutMs) {
	        return consumer.poll(timeoutMs);
	    }

	    private static void loadConfigProperties(){
	        config = new Properties();
	        InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream(CONSUMER_PROPS);
	        if (is != null) {
	            try {
	                config.load(is);

	            } catch (IOException e) {
	                fail("Exception reading " + CONSUMER_PROPS + ": " + e.toString());
	            }
	        }

	    }

	    private static void setupBootstrapAndDeSerializers(Properties properties){
	        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty(ItemConsumerConfig.BOOTSTRAP_SERVERS));
	        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getProperty(ItemConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
	        properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.getProperty(ItemConsumerConfig.GROUP_ID_CONFIG));
	        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, config.getProperty(ItemConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG));
	        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getProperty(ItemConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
	        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ItemDeserializer.class.getName());
	    }

		public ItemConsumerRebalanceListener getListener() {
			return listener;
		}

		public void setListener(ItemConsumerRebalanceListener listener) {
			this.listener = listener;
		}
       
	  public void close() {
	        consumer.close();
	    }
}

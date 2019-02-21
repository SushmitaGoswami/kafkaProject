package com.kafka.consumer;

import com.kafka.config.ItemConsumerConfig;
import com.kafka.model.Item;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.fail;

public class ItemConsumer {
    private final static String CONSUMER_PROPS = "consumerConfig.properties";
    private KafkaConsumer<String, Item> consumer;
    private final String clientId;
    private List<String> topics;
    private
    private static Properties config;

    public ItemConsumer(String clientId, List<String> topics) {

        final Properties props = new Properties();
        loadConfigProperties();
        setupBootstrapAndDeSerializers(props);
        this.clientId = clientId;
        this.topics = topics;
        this.consumer = new KafkaConsumer<String, Item>(props);
        this.consumer.subscribe(topics);
        getRebalanceListener();
    }

    private ConsumerRebalanceListener getRebalanceListener(){
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        };
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

}

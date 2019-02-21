package com.kafka.producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import static org.junit.Assert.fail;

import com.kafka.config.ItemProducerConfig;
import com.kafka.partitioner.ItemPartitioner;
import com.kafka.serializer.ItemSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import com.kafka.model.Item;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class ItemProducer {
    private final static String BROKER_PROPS = "producerConfig.properties";
    private static Properties config ;
    public static Producer<String, Item> createProducer(String topicName) {

        final Properties props = new Properties();
        loadConfigProperties();
        setupBootstrapAndSerializers(props, topicName);
        setupBatchingAndCompression(props);
        setupRetriesInFlightTimeout(props);
        setupCustomPartitioner(props);
        return new KafkaProducer(props);
    }

    private static void loadConfigProperties(){
        config = new Properties();
        InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream(BROKER_PROPS);
        if (is != null) {
            try {
                config.load(is);

            } catch (IOException e) {
                fail("Exception reading " + BROKER_PROPS + ": " + e.toString());
            }
        }

    }
    private static void setupBootstrapAndSerializers(final Properties props, String topicName) {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ItemProducerConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, topicName);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Custom Serializer - config "value.serializer"
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ItemSerializer.class.getName());
    }

    private static void setupBatchingAndCompression(final Properties props) {
        //Linger up to 100 ms before sending batch if size not met
        props.put(ProducerConfig.LINGER_MS_CONFIG, (int)(config.get(ItemProducerConfig.LINGER_MS_CONFIG)));

        //Batch up to 64K buffer sizes.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,  (int)(config.get(ItemProducerConfig.BATCH_SIZE_CONFIG)));

        //Use Snappy compression for batch compression.
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.get(ItemProducerConfig.COMPRESSION_TYPE_CONFIG));
    }

    private static void setupRetriesInFlightTimeout(Properties props) {
        //Only one in-flight messages per Kafka broker connection
        // - max.in.flight.requests.per.connection (default 5)
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, (int)(config.get(ItemProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)));
        //Set the number of retries - retries
        props.put(ProducerConfig.RETRIES_CONFIG, (int)(config.get(ItemProducerConfig.RETRIES_CONFIG)));

        //Request timeout - request.timeout.ms
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int)(config.get(ItemProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)));

        //Only retry after one second.
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, (int)(config.get(ItemProducerConfig.RETRY_BACKOFF_MS_CONFIG)));
    }

    private static void setupCustomPartitioner(Properties properties){
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, ItemPartitioner.class.getName());
    }
}
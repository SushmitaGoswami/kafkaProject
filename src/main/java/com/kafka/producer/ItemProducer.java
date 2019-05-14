package com.kafka.producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import static org.junit.Assert.fail;

import com.kafka.common.ItemSerializer;
import com.kafka.config.ItemProducerConfig;
import com.kafka.partitioner.ItemPartitioner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import com.kafka.model.Item;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class ItemProducer{
	
    private final static String BROKER_PROPS = "producerConfig.properties";
    private static Properties config ;
    
    public static Producer<String, Item> createProducer() {

        final Properties props = new Properties();
        loadConfigProperties();
        setupBootstrapAndSerializers(props);
        setupBatchingAndCompression(props);
        setupRetriesInFlightTimeout(props);
        setupCustomPartitioner(props);
        return new KafkaProducer<String, Item>(props);
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
    private static void setupBootstrapAndSerializers(final Properties props) {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ItemProducerConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Custom Serializer - config "value.serializer"
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ItemSerializer.class.getName());
      //Set number of acknowledgments - acks - default is all
        props.put(ProducerConfig.ACKS_CONFIG, config.get(ItemProducerConfig.ACKS_CONFIG));
    }

    private static void setupBatchingAndCompression(final Properties props) {
        //Linger up to specified ms before sending batch if size not met
        props.put(ProducerConfig.LINGER_MS_CONFIG, (int)(config.get(ItemProducerConfig.LINGER_MS_CONFIG)));

        //Batch up to specified buffer sizes.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,  (int)(config.get(ItemProducerConfig.BATCH_SIZE_CONFIG)));

        //Use specified compression for batch compression.
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.get(ItemProducerConfig.COMPRESSION_TYPE_CONFIG));
        
        //memory available to a producer for buffering. If records get sent faster than they can be transmitted to Kafka then and this buffer will get exceeded
        // and producer will be blocked
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.get(ItemProducerConfig.BUFFER_MEMORY_CONFIG));
        
        //how much time producer remain blocked before sending the TimeOutException
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, config.get(ItemProducerConfig.MAX_BLOCK_MS_CONFIG));
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
    	// set up custom partitioner
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, ItemPartitioner.class.getName());
    }
    
}
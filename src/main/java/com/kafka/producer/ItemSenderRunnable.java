package com.kafka.producer;

import com.kafka.model.Item;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ItemSenderRunnable<K,V extends Item> implements Runnable{
	private String topicName;
    private Producer<K, V> producer;
    private List<V> itemList;
    
    public ItemSenderRunnable(Producer<K,V> producer, String topicName, List<V> itemList)
    {
        this.producer = producer;
        this.topicName = topicName;
        this.itemList = itemList;
        
    }

    private void displayRecordMetaData(final ProducerRecord<K, V> record,
                                       final Future<RecordMetadata> future) throws InterruptedException, ExecutionException
             {
        final RecordMetadata recordMetadata = future.get();
        System.out.println((String.format("\n\t\t\tkey=%s, value=%s " +
                        "\n\t\t\tsent to topic=%s part=%d off=%d at time=%s",
                record.key(),
                record.value().toJson(),
                recordMetadata.topic(),
                recordMetadata.partition(),
                recordMetadata.offset(),
                new Date(recordMetadata.timestamp())
        )));
    }

    @Override
    public void run() {
        int sentCount = 0;
        final Random random = new Random(itemList.size());
        while(true) {
            try {
				@SuppressWarnings("unchecked")
				ProducerRecord<K, V> record = (ProducerRecord<K, V>) createRandomItem(random);
                
                final Future<RecordMetadata> future = producer.send(record, new ItemProducerCallBack());
                if (sentCount % 100 == 0) {
                    displayRecordMetaData(record, future);
                }
            } catch (InterruptedException e) {
                if (Thread.interrupted()) {
                    break;
                }
            } catch (ExecutionException e) {
                System.out.println("problem sending record to producer " + e.getMessage());
                e.printStackTrace();
            }
        }
        sentCount++;
    }
    
    private ProducerRecord<?,?> createRandomItem(Random random) {
    	V item = itemList.get(random.nextInt());
        if(item.isSpecial()) {
        	return new ProducerRecord<>(this.topicName,"special",item);		
        }
        else {
        	return new ProducerRecord<>(this.topicName,"usual",item);
        }
    	
    }
}

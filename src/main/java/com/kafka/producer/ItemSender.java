package com.kafka.producer;

import com.kafka.model.Item;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ItemSender implements Runnable{
    private Producer producer;

    public ItemSender(Producer producer)
    {
        this.producer = producer;
    }

    private void displayRecordMetaData(final ProducerRecord<String, Item> record,
                                       final Future<RecordMetadata> future)
            throws InterruptedException, ExecutionException {
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
        while(true) {
            try {
                ProducerRecord<String, Item> record = new ProducerRecord<>("laptop", new Item(1,"dell"));
                final Future<RecordMetadata> future = producer.send(record);
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
}

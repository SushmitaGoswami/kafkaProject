package com.kafka.serializer;

import com.kafka.model.Item;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ItemSerializer implements Serializer<Item> {

        @Override
        public byte[] serialize(String topic, Item data) {
            return data.toJson().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public void close() {
        }
    }

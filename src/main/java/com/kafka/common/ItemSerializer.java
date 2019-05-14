package com.kafka.common;

import com.google.gson.Gson;
import com.kafka.model.Item;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ItemSerializer implements Serializer<Item> {
	    private Charset charset = StandardCharsets.UTF_8;

	    @Override
	    public void configure(Map<String, ?> configs, boolean isKey) {
	    }
    
        @Override
        public byte[] serialize(String topic, Item data) {
        	Gson gson = new Gson();
        	if(data == null)
        		return null;
            return gson.toJson(data).getBytes(charset);
        }

        @Override
        public void close() {
        }
    }

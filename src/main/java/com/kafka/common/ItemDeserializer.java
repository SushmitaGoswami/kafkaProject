package com.kafka.common;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.Gson;
import com.kafka.model.Item;

public class ItemDeserializer implements Deserializer<Item>{

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Item deserialize(String topic, byte[] data) {
		if(data == null)
			return null;
		Gson gson = new Gson();
		Item item = gson.fromJson(new String(data), Item.class);
		return item;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}

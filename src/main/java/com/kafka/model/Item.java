package com.kafka.model;

public class Item {
    private int id;
    private String name;
    private boolean isSpecial;

    public Item(int id, String name, boolean isSpecial) {
        this.id = id;
        this.name = name;
        this.isSpecial = isSpecial;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    

    public boolean isSpecial() {
		return isSpecial;
	}

	public void setSpecial(boolean isSpecial) {
		this.isSpecial = isSpecial;
	}

	public String toJson() {
        return "{" +
                "\"id\": " + id +
                ", \"name\": \"" + name + '\"' +
                ", \"isSpecial\": \"" + isSpecial + '\"' +
                '}';
    }

}

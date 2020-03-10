package com.chm.flink.pojo;

import java.io.Serializable;
import java.util.Objects;

public class Event implements Serializable {
    public String name;
    public int price;
    public String type;
    public String timestamp;

    public Event() {
    }

    public Event(String name ,int price, String type) {
        this.type = type;
        this.name = name;
        this.price = price;
        timestamp = System.currentTimeMillis()+"";
    }

    @Override
    public String toString() {
        return name+","+price+","+type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return price == event.price &&
                Objects.equals(name, event.name) &&
                Objects.equals(type, event.type) &&
                Objects.equals(timestamp, event.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, price, type, timestamp);
    }

    public String getName() {
        return name;
    }

    public int getPrice() {
        return price;
    }

    public String getType() {
        return type;
    }


    public static void main(String[] args) {


    }

}

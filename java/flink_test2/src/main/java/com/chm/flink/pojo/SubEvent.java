package com.chm.flink.pojo;

import java.util.Objects;

public class SubEvent extends Event {
    public String name;
    public int price;
    public String type;

    public SubEvent() {
    }

    public SubEvent(String type , String name, int price) {
        this.type = type;
        this.name = name;
        this.price = price;
    }

    @Override
    public String toString() {
        return "SubEvent " + name + " " + price + " " + type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubEvent event = (SubEvent) o;
        return price == event.price &&
                Objects.equals(name, event.name) &&
                Objects.equals(type, event.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, price, type);
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
}
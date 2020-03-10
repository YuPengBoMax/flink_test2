package com.chm.flink.pojo;

import java.util.Objects;

public class OutTimeEvent{
    private String userId;//用户ID
    private String type;//登录类型
    private String timestamp;//登录IP

    public OutTimeEvent(String userId, String type, String timestamp) {
        this.userId = userId;
        this.timestamp = timestamp;
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutTimeEvent that = (OutTimeEvent) o;
        return userId.equals(that.userId) &&
                type.equals(that.type) &&
                timestamp.equals(that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, type, timestamp);
    }

    @Override
    public String toString() {
        return "OutTimeEvent{" +
                "userId='" + userId + '\'' +
                ", type='" + type + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}

package com.chm.flink.pojo;

import java.io.Serializable;

public class LoginEvent implements Serializable {
    private String userId;//用户ID
    private String type;//登录类型
    private String timestamp;//登录IP

    public LoginEvent() {
    }

    public LoginEvent(String userId, String ip, String type) {
        this.userId = userId;
        this.timestamp = ip;
        this.type = type;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getType() {
        return type;
    }

    public String getUserId() {
        return userId;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ip='" + timestamp + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}

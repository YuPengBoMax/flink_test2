package com.chm.flink.pojo;

import java.io.Serializable;

public  class LoginWarning implements Serializable {
    private String userId;
    private String type;
    private String timestamp;

    public LoginWarning() {
    }

    public LoginWarning(String userId, String type, String timestamp) {
        this.userId = userId;
        this.type = type;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LoginWarning{" +
                "userId='" + userId + '\'' +
                ", type='" + type + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}

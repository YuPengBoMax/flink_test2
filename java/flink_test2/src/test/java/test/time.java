package test;

import com.alibaba.fastjson.JSONObject;

import java.util.Date;
import java.util.Random;

public class time {
    public static void main(String[] args) {
        Random random = new Random();
        String key = "" + (char) ('A' + random.nextInt(5));
        long timestamp = new Date().getTime();
        JSONObject json = new JSONObject();
        json.put("type",key);
        json.put("timestamp",timestamp);
        String jsonstr = json.toString();
        JSONObject jsonObject = (JSONObject) JSONObject.parse((String) jsonstr);
        System.out.println(jsonObject.get("timestamp"));

    }
}
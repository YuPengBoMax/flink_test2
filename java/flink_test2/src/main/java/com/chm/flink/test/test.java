package com.chm.flink.test;

import com.chm.flink.pojo.LoginEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class test {

    public static void main(String[] args) {
        Map<String, List<LoginEvent>> map = new HashMap<>();
        List<LoginEvent> list = new ArrayList<>();
        list.add(new LoginEvent("a","a1","a1"));
        list.add(new LoginEvent("a","a2","a2"));
        list.add(new LoginEvent("a","a3","a3"));
        list.add(new LoginEvent("a","a4","a4"));
        map.put("1",list);
        //map.values().stream().flatMap(elems->elems.stream().collect());
        System.out.println("sfsdafs".compareTo("sfdsfd"));


    }
}

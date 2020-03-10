package com.chm.flink.StreamSplit;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import java.util.ArrayList;
import java.util.List;

public class Splitoutput implements OutputSelector<Integer> {
    public Iterable<String> select(Integer value) {
        List<String> list = new ArrayList<>();
        if (value > 5) {
            list.add(">5");
        } else {
            list.add("<=5");
        }
        return list;
    }

}

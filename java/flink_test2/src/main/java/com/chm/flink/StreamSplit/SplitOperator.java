package com.chm.flink.StreamSplit;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class SplitOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(2);

        DataStreamSource<String> source = sEnv.socketTextStream("47.52.108.102", 9999);
        SplitStream<Integer> splitStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                try {
                    Integer.valueOf(s);
                    return true;
                } catch (Exception e) {
                    System.out.println("Exception");
                    return false;
                }
            }
        }).map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.valueOf(value);
            }
        }).split(new Splitoutput());
        /*.split(new OutputSelector<Integer>() { // split可以将一个流，通过打Tag的方式，split成多个流
            public Iterable<String> select(Integer value) {
                List<String> list = new ArrayList<>();
                if (value > 5) {
                    list.add(">5");
                } else {
                    list.add("<=5");
                }
                return list;
            }
        });*/


        // SplitStream流 通过select("tag")获取DataStream流
        DataStream<Integer> five1 = splitStream.select(">5");
        DataStream<Integer> five2 = splitStream.select("<=5");
        five1.print();
        five2.print();

        // 将流合并
        /*DataStream<Integer> union = five1.union(five2);
        union.print();*/

        sEnv.execute("SplitOperator");
    }
}

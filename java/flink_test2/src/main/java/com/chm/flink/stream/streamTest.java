package com.chm.flink.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class streamTest {

    public static void main(String[] args) {
        String filePath = "C:\\Users\\SSAAS\\Desktop\\one.txt";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.setParallelism(1);
        //DataStream<String> ds = env.socketTextStream("47.52.108.102", 9001);
        DataStreamSource<String> ds = env.readTextFile(filePath);
        ds.print();
        ds.map(new MapFunction<String, String[]>() {
            @Override
            public String[] map(String s) throws Exception {
                return s.substring(1, s.length() - 1).split(",");
            }
        }).map(new MapFunction<String[], Tuple6<String, String, String, String, String, String>>() {
            public Tuple6<String, String, String, String, String, String> map(String[] one) {
                return new Tuple6<>(
                        one[0].replace("\"", "").split(":")[1],
                        one[1].replace("\"", "").split(":")[1],
                        one[2].replace("\"", "").split(":")[1],
                        one[3].replace("\"", "").split(":")[1],
                        one[4].replace("\"", "").split(":")[1],
                        one[5].replace("\"", "").split(":")[1]);
            }
        }).print();

        /*
        DataStream<Tuple2<String, Integer>> ds1 = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] ss = value.split("\\W+");
                for (String s : ss) {
                    out.collect(Tuple2.of(s,1));
                }
            }
        });
        ds1.keyBy(0).sum(1).print();*/
        try {
            env.execute("adaf");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

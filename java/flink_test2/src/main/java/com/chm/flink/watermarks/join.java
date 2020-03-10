package com.chm.flink.watermarks;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class join {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds1 = env.socketTextStream("47.52.108.102", 9000);
        DataStream<String> ds2 = env.socketTextStream("47.52.108.102", 9001);

        DataStream<Tuple2<String, String>> ds3 = ds1.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                String[] ss = value.split("\\W+");
                for (String s : ss) {
                    out.collect(Tuple2.of(s, s + 9000 + s));
                }
            }
        });
        DataStream<Tuple2<String, String>> ds4 = ds2.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                String[] ss = value.split("\\W+");
                for (String s : ss) {
                    out.collect(Tuple2.of(s, s + 9001 + s));
                }
            }
        });
        /*ds3.print();
        ds4.print();*/
        DataStream<String> result = ds3.join(ds4).where(new KeySelector<Tuple2<String, String>, Object>() {
            public Object getKey(Tuple2<String, String> t1) throws Exception {
                return t1.f0;
            }
        }).equalTo(new KeySelector<Tuple2<String, String>, Object>() {
            public Object getKey(Tuple2<String, String> t2) throws Exception {
                return t2.f0;
            }
        }).window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, String>() {
                           public String join(Tuple2<String, String> stringIntegerTuple2, Tuple2<String, String> stringIntegerTuple22) throws Exception {
                               return stringIntegerTuple2.f1 + "|" + stringIntegerTuple22.f1;
                           }
                       }
                );
        result.print();

        try {
            env.execute("adaf");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

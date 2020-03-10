package com.chm.flink.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class stream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds1 = env.socketTextStream("47.52.108.102", 9000);
        KeyedStream<Tuple2<String, Integer>, Tuple> ds11 = ds1.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) {
                if(s.startsWith("0"))
                    return Tuple2.of(s, 0);
                else
                    return Tuple2.of(s,1);
            }
        }).keyBy(0);
        /*DataStream<String> ds2 = env.socketTextStream("rh-bigdata-002", 9001);
        DataStream<Tuple2<String, Integer>> ds22 = ds2.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) {
                return new Tuple2<>(s, 9001);
            }
        });*/

        /*ds11.keyBy(1).fold("start",new FoldFunction<Tuple2<String, Integer>, String>(){
            @Override
            public String fold(String s, Tuple2<String, Integer> o) throws Exception {
                return s + "-" + o.f0;
            }
        });*/

        /*ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> cs = ds22.connect(ds22);
        cs.keyBy(1,0).getFirstInput().print();*/

        ds11.min(0).print();
        ds11.minBy(0).print();

        try {
            env.execute("sdf");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

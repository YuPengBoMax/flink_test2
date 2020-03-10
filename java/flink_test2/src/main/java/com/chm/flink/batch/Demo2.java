package com.chm.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Demo2 {

    public static void main(String[] args) throws  Exception{

        String filePath = "C:\\Users\\SSAAS\\Desktop\\a.txt";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> ds = env.readTextFile(filePath);
        DataSource<Long> ds1 = env.fromElements(1L, 2L, 3L, 4L, 7L, 5L, 1L, 5L, 4L, 6L, 1L, 7L, 8L, 9L, 1L);
        ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                String[] ss = value.split("\\W+");
                for (String s : ss) {
                    out.collect(Tuple2.of(s,1));
                }
            }
        });
        ds.print();


    }

}

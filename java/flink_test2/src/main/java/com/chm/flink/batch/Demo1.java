package com.chm.flink.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo1 {
    public static void main(String[] args) throws Exception {
        String filePath = "C:\\Users\\SSAAS\\Desktop\\one.txt";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> ds = env.readTextFile(filePath);
        DataSet<String[]> ds1;
        DataSet<Tuple6<String, String, String, String, String, String>> ds2;
        ds.print();

        ds1 = ds.map(one -> one.substring(1, one.length() - 1).split(","));
        ds2 = ds1.map(new MapFunction<String[], Tuple6<String, String, String, String, String, String>>() {
            public Tuple6<String, String, String, String, String, String> map(String[] one) {
                return new Tuple6<>(
                        one[0].replace("\"", "").split(":")[1],
                        one[1].replace("\"", "").split(":")[1],
                        one[2].replace("\"", "").split(":")[1],
                        one[3].replace("\"", "").split(":")[1],
                        one[4].replace("\"", "").split(":")[1],
                        one[5].replace("\"", "").split(":")[1]);
            }
        });

        ds2.print();
            /*ds2 = ds.map( new MapFunction<String, Tuple6<String, String, String, String, String, String>>() {
                public Tuple6<String, String, String, String, String, String> map(String json) {
                    String[] param = json.substring(1, json.length() - 1).split(",");
                    return new Tuple6<>(
                            param[0].replace("\"", "").split(":")[1],
                            param[1].replace("\"", "").split(":")[1],
                            param[2].replace("\"", "").split(":")[1],
                            param[3].replace("\"", "").split(":")[1],
                            param[4].replace("\"", "").split(":")[1],
                            param[5].replace("\"", "").split(":")[1]);
                }
            });*/
        ds2.print();


    }
}

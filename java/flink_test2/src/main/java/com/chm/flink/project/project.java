package com.chm.flink.project;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.ProjectOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class project {

    public static void main(String[] args) throws Exception {
        String filePath1 = "C:\\Users\\SSAAS\\Desktop\\time.txt";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> ds = env.readTextFile(filePath1);
        ds.print();
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.addSource(new com.chm.flink.Source_Sink.DataSource2()).print();

        FlatMapOperator<String, Tuple3<String, Integer, Integer>> fmo = ds.flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Integer>>() {
            public void flatMap(String value, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                String[] ss = value.split("\\W+");
                for (String s : ss) {
                    out.collect(Tuple3.of(s, 1, 2));
                }
            }
        });
        ProjectOperator<?, Tuple> project = fmo.project(1,2);
        project.print();
        fmo.print();
        senv.execute("sfd");



    }

}

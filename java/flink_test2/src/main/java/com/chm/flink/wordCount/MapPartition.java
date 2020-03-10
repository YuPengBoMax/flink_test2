package com.chm.flink.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class MapPartition {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

       /* if(args.length != 3){
            System.out.println("the use is   input  output parallelism");
            return;
        }
        //输入
        String input = args[0];
        String output = args[1];
        //并行度
        int parallelism = Integer.parseInt(args[2]);*/


        String input = "C:\\Users\\SSAAS\\Desktop\\a.txt";
        String output = "C:\\Users\\SSAAS\\Desktop\\result";
        int parallelism = 2;
        //设置并行度
        env.setParallelism(parallelism);
        DataSet<String> text = env.readTextFile(input);
        text.print();
        DataSet<Tuple2<String, Integer>> words = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] strings = value.split("\\W+");
                for (String s : strings) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        });

        ReduceOperator<Tuple2<String, Integer>> result = words.groupBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return Tuple2.of(t1.f0, t1.f1 + t2.f1);
            }
        });
        result.first(3);

        /*//进行Range-Partition
        DataSet<Tuple2<String,Integer>> RangeWords = words.partitionByRange(0);
        DataSet<Tuple2<String,Integer>> counts = RangeWords.groupBy(0).sum(1);
        //写出结果
        counts.writeAsText(output);*/
        env.execute("this is a range partition job!!!");
    }
}

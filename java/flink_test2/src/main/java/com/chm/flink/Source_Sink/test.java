package com.chm.flink.Source_Sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class test {
    public static void main(String[] args)  throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> ds1 = env.addSource(new DataSource2());
        SingleOutputStreamOperator<Tuple2<Long, Integer>> ss = ds1.map(new MapFunction<Long, Tuple2<Long, Integer>>() {
            @Override
            public Tuple2<Long, Integer> map(Long aLong) throws Exception {
                return Tuple2.of(aLong,1);
            }
        });
        ss.addSink(new DataSink()).setParallelism(2);

        env.execute("ad");
    }
}

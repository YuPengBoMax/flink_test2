package com.chm.flink.AtLeaseOnce;

import com.chm.flink.Source_Sink.DataSource2;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class process {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        DataStreamSource<Long> ds = env.addSource(new DataSource2());
        SingleOutputStreamOperator<String> ds1 = ds.map(new RichMapFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return String.valueOf(aLong);
            }
        });
        ds1.addSink(new MultiThreadConsumerSink());
        env.execute("aaa");
    }
}

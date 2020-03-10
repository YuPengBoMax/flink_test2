package com.chm.flink.checkpoint;

import com.chm.flink.Source_Sink.DataSink;
import com.chm.flink.Source_Sink.DataSink1;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class dk {

    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置checkpoint间隔时间和处理语义
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint目录
        env.setStateBackend(new FsStateBackend("file:///C:\\Users\\SSAAS\\Desktop\\checkpoint"));
        // 重启策略  重启5次 每次间隔30秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 30000)); //


        //开启 EventTime 时间模型
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> ds = env.socketTextStream("47.52.108.102", 9000);


        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = ds.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            public void open(Configuration parameters) throws Exception {
                System.out.println("open");
            }

            public Tuple2 map(String val) throws Exception {
                return Tuple2.of(val, 1);
            }

            public void close() throws Exception {
                System.out.println("close");
            }
        });

        /* ds1.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            public String getKey(Tuple2<String, Integer> var1) throws Exception{
                return var1.f0;
            }
        }); */

        ds1.addSink(new DataSink1()).name("1");
        ds1.addSink(new DataSink1()).name("2");
        try {
            env.execute("sfda");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

package com.chm.flink.Source_Sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class DataSink1 implements SinkFunction<Tuple2<String, Integer>>{
    public void invoke(Tuple2<String, Integer> val, Context context) throws Exception {
        // 每个类型的商品成交量
        System.out.println("2  " + val.f0 + " " + val.f1);
        //System.out.println(context.currentProcessingTime());
        //System.out.println(context.currentWatermark());
        //System.out.println(context.timestamp());
    }
}

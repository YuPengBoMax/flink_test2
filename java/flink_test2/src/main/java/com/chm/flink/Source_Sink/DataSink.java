package com.chm.flink.Source_Sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.LinkedList;
import java.util.List;

public class DataSink extends RichSinkFunction<Tuple2<Long, Integer>> implements CheckpointedFunction {
    private  List<Tuple2<Long,Integer>> list;

    public void open(Configuration parameters) throws Exception {
        this.list  = new LinkedList<>();
    }

    public void close() throws Exception {
    }


    public void invoke(Tuple2<Long, Integer> val,SinkFunction.Context context) throws Exception {
        // 每个类型的商品成交量
        list.add(val);
        if(list.size()>10){
            System.out.println("list size > 10 ");
            for (Tuple2 value : list){
                System.out.println(value);
            }
            list.clear();
        }
        System.out.println("sink    ");
        //System.out.println(context.currentProcessingTime());
        //System.out.println(context.currentWatermark());
        //System.out.println(context.timestamp());
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}

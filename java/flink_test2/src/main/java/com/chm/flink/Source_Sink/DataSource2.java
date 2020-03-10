package com.chm.flink.Source_Sink;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;

public class DataSource2 extends RichParallelSourceFunction<Long> {
    private volatile boolean isRunning = true;


    public void run(SourceFunction.SourceContext<Long> ctx) throws Exception {
        Random random = new Random();
        while (isRunning){
            Thread.sleep(300);
            int a = random.nextInt(10);
            ctx.collect(new Long((long)a));
        }

    }

    public void cancel() {
        isRunning = false;
    }



}
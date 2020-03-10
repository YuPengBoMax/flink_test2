package com.chm.flink.Source_Sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class DataSource3  implements SourceFunction<Tuple3<Integer,String,Long>>{
    private volatile boolean isRunning = true;


    public void run(SourceFunction.SourceContext<Tuple3<Integer,String,Long>> ctx) throws Exception {
        Random random = new Random();
        while (isRunning){
            Thread.sleep(300);
            int id = random.nextInt(10);
            long frequency = random.nextInt(10);
            String word = "" + (char)('A'+random.nextInt(10));
            ctx.collect(Tuple3.of(id,word,frequency));
        }

    }

    public void cancel() {
        isRunning = false;
    }
}

package com.chm.flink.Source_Sink;

import com.chm.flink.tablesql.sqlToMysql.WC;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class DataSource4 implements SourceFunction<WC> {
    private volatile boolean isRunning = true;


    public void run(SourceFunction.SourceContext<WC> ctx) throws Exception {
        Random random = new Random();
        while (isRunning){
            Thread.sleep(300);
            int id = random.nextInt(10);
            long frequency = random.nextInt(10);
            String word = "" + (char)('A'+random.nextInt(10));
            ctx.collect(new WC(id,word,frequency));
        }

    }

    public void cancel() {
        isRunning = false;
    }
}


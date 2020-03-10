package com.chm.flink.Source_Sink;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.text.SimpleDateFormat;
import java.util.Random;

public class DataSource1 extends RichParallelSourceFunction<String> {
    private volatile boolean isRunning = true;


    public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static Random random = new Random();
    public void run(SourceContext<String> ctx) throws Exception {

        while (isRunning){
            Thread.sleep(1000);
            long timestamp = System.currentTimeMillis();
            if((timestamp/1000)%5==0){
                timestamp = timestamp-5500;
            }

            char a = (char) ('A' + random.nextInt(20));

            //System.out.println(timestamp);
            ctx.collect(a + " " + timestamp);


        }

    }

    public void cancel() {
        isRunning = false;
    }



}

package com.chm.flink.Source_Sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class DataSource extends RichParallelSourceFunction<JSONObject> {
    private volatile boolean isRunning = true;


    public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public void run(SourceContext<JSONObject> ctx) throws Exception {

        Random random = new Random();
        while (isRunning) {
            Date date = new Date();
            String time = format.format(date);
            long timestamp = date.getTime();

            try {
                Thread.sleep((2000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            JSONObject json = new JSONObject();

           /* if(timestamp%5 == 0) {
                try {
                    Thread.sleep((5000));
                    json.put("zzz" , 1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }*/

            String type = "" + (char)('A' + random.nextInt(3));
            type = "a";
            json.put("type",type);
            json.put("time",time);
            json.put("timestamp",timestamp);

            ctx.collect(json);
        }

    }

    public void cancel() {
        isRunning = false;
    }



}

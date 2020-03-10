package com.chm.flink.Source_Sink;

import com.chm.flink.pojo.LoginEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class DataSource6 implements SourceFunction<LoginEvent> {
    private volatile boolean isRunning = true;


    public void run(SourceContext<LoginEvent> ctx) throws Exception {
        Random random = new Random();
        while (isRunning){
            Thread.sleep(300);
            String Userid = random.nextInt(3)+"";
            String ip = "192.168.0."+random.nextInt(3);
            String type = "" + (char)('A'+random.nextInt(3));
            ip = System.currentTimeMillis()/1000*1000+"";
            ctx.collect(new LoginEvent(Userid,ip,type));
        }

    }

    public void cancel() {
        isRunning = false;
    }
}
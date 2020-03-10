package com.chm.flink.Source_Sink;

import com.chm.flink.pojo.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class DataSource5 implements SourceFunction<Event> {
    private volatile boolean isRunning = true;


    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        while (isRunning){
            Thread.sleep(100);

            String type = "" + (char)('A'+random.nextInt(10));
            int price = random.nextInt(5);
            String name = "" + (char)('A'+random.nextInt(10));
            name = name + (char)('A'+random.nextInt(10));

            Event event = new Event(name,price,type);

            ctx.collect(event);
        }

    }

    public static void main(String[] args) throws  Exception{

        Random random = new Random();
        while (true) {

            int price = random.nextInt(5);
            String name = "" + (char) ('A' + random.nextInt(5));
            String type = "" + (char) ('A' + random.nextInt(5));
            Thread.sleep(100);
            name = name + (char) ('A' + random.nextInt(20));
            Event event = new Event(name, price, type);
            System.out.println(event.toString());
        }

    }

    public void cancel() {
        isRunning = false;
    }
}


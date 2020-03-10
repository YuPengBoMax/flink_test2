package test;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class blockqueue {

    private static LinkedBlockingQueue<String> bufferQueue = new LinkedBlockingQueue<>();

    public static void main(String[] args) {

        ExecutorService threadPool = Executors.newCachedThreadPool();

        threadPool.execute(new Runnable() {
            Random random = new Random();

            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(700);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    String a = "" + random.nextInt(100);
                    bufferQueue.offer(a);
                }
            }
        });
        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                    while (true) {
                        Thread.sleep(1000);
                        System.out.println(System.currentTimeMillis() + "  "+ Thread.currentThread().getName()+" "+bufferQueue.poll());
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                    while (true) {
                        Thread.sleep(1000);
                        System.out.println(System.currentTimeMillis() + "  "+ Thread.currentThread().getName()+" "+bufferQueue.poll());
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


    }
}

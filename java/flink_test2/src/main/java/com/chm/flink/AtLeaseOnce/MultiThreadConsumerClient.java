package com.chm.flink.AtLeaseOnce;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;

public class MultiThreadConsumerClient implements Runnable {

    private LinkedBlockingQueue<String> bufferQueue;
    private CyclicBarrier clientBarrier;

    public MultiThreadConsumerClient(LinkedBlockingQueue<String> bufferQueue, CyclicBarrier clientBarrier) {
        this.bufferQueue = bufferQueue;
        this.clientBarrier = clientBarrier;
    }

    @Override
    public void run() {
        String entity;
        while (true) {
            // 从 bufferQueue 的队首消费数据
            entity = bufferQueue.poll();
            // 执行 client 消费数据的逻辑
            if (entity != null) {
                doSomething(entity);
            } else {
                try {
                    int a = clientBarrier.getNumberWaiting();
                    if(a>0){
                        System.out.println(Thread.currentThread().getName() + "  NumberWaiting  " + a);
                        clientBarrier.await();
                        System.out.println(Thread.currentThread().getName() + "  " + "flush ok");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // client 消费数据的逻辑
    private void doSomething(String entity) {
        // client 积攒批次并调用第三方 api
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName() + "  " + entity);
    }
}
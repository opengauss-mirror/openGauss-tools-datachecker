package com.gauss.controller;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * 简单利用信号量控制table并发数
 */
public class TableController {

    private CountDownLatch                      countLatch;
    private Semaphore                           sem;
    private LinkedBlockingQueue<GaussInstance> queue = new LinkedBlockingQueue<GaussInstance>();

    public TableController(int total, int cocurrent){
        this.countLatch = new CountDownLatch(total);
        this.sem = new Semaphore(cocurrent);
    }

    public void acquire() throws InterruptedException {
        sem.acquire();
    }

    public void release(GaussInstance instance) {
        sem.release();
        queue.offer(instance);
        countLatch.countDown();
    }

    public GaussInstance takeDone() throws InterruptedException {
        return queue.take();
    }

    public void waitForDone() throws InterruptedException {
        countLatch.await();
    }

}

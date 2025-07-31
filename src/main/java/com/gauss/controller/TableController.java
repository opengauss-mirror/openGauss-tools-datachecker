/*
This program is free software;
you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program;
if not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
*/

package com.gauss.controller;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * control the concurrency between tables
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

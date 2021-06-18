package com.gauss.common.utils.thread;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 多线程执行器模板代码
 */
public class ExecutorTemplate {

    private volatile ExecutorCompletionService comletions = null;
    private volatile List<Future>              futures    = null;

    public ExecutorTemplate(ThreadPoolExecutor executor){
        futures = Collections.synchronizedList(new ArrayList<Future>());
        comletions = new ExecutorCompletionService(executor);
    }

    public void submit(Runnable task) {
        Future future = comletions.submit(task, null);
        futures.add(future);
        check(future);
    }

    public void submit(Callable<Exception> task) {
        Future future = comletions.submit(task);
        futures.add(future);
        check(future);
    }

    private void check(Future future) {
        if (future.isDone()) {
            // 立即判断一次，因为使用了CallerRun可能当场跑出结果，针对有异常时快速响应，而不是等跑完所有的才抛异常
            try {
                future.get();
            } catch (Throwable e) {
                // 取消完之后立马退出
                cacelAllFutures();
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized List<?> waitForResult() {
        List result = new ArrayList();
        RuntimeException exception = null;
        // 开始处理结果
        int index = 0;
        while (index < futures.size()) { // 循环处理发出去的所有任务
            try {
                Future future = comletions.take();// 它也可能被打断
                result.add(future.get());
            } catch (Throwable e) {
                exception = new RuntimeException(e);
                break;
            }

            index++;
        }

        if (exception != null) {
            // 小于代表有错误，需要对未完成的记录进行cancel操作，对已完成的结果进行收集，做重复录入过滤记录
            cacelAllFutures();
            throw exception;
        } else {
            return result;
        }
    }

    public void cacelAllFutures() {
        for (Future future : futures) {
            if (!future.isDone() && !future.isCancelled()) {
                future.cancel(true);
            }
        }
    }

    public void clear() {
        futures.clear();
    }

}

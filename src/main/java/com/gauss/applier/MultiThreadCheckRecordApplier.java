package com.gauss.applier;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.MDC;

import com.gauss.common.GaussConstants;
import com.gauss.common.model.GaussContext;
import com.gauss.common.model.record.Record;
import com.gauss.common.utils.GaussUtils;
import com.gauss.common.utils.thread.ExecutorTemplate;
import com.gauss.common.utils.thread.NamedThreadFactory;
import com.gauss.exception.GaussException;

public class MultiThreadCheckRecordApplier extends CheckRecordApplier {

    private int                threadSize = 5;
    private int                splitSize  = 50;
    private ThreadPoolExecutor executor;
    private String             executorName;

    public MultiThreadCheckRecordApplier(GaussContext context){
        super(context);
    }

    public MultiThreadCheckRecordApplier(GaussContext context, int threadSize, int splitSize){
        super(context);

        this.threadSize = threadSize;
        this.splitSize = splitSize;
    }

    public MultiThreadCheckRecordApplier(GaussContext context, int threadSize, int splitSize,
                                         ThreadPoolExecutor executor){
        super(context);

        this.threadSize = threadSize;
        this.splitSize = splitSize;
        this.executor = executor;
    }

    public void start() {
        super.start();

        executorName = this.getClass().getSimpleName() + "-" + context.getTableMeta().getFullName();
        if (executor == null) {
            executor = new ThreadPoolExecutor(threadSize,
                threadSize,
                60,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue(threadSize * 2),
                new NamedThreadFactory(executorName),
                new ThreadPoolExecutor.CallerRunsPolicy());
        }
    }

    public void apply(final List<Record> records) throws GaussException {
        // no one,just return
        if (GaussUtils.isEmpty(records)) {
            return;
        }

        if (records.size() > splitSize) {
            ExecutorTemplate template = new ExecutorTemplate(executor);
            try {
                int index = 0;
                int size = records.size();
                for (; index < size;) {
                    int end = Math.min(index + splitSize, size);
                    final List<Record> subList = records.subList(index, end);
                    template.submit(new Runnable() {

                        public void run() {
                            String name = Thread.currentThread().getName();
                            try {
                                MDC.put(GaussConstants.MDC_TABLE_SHIT_KEY, context.getTableMeta().getFullName());
                                Thread.currentThread().setName(executorName);
                                doApply(subList);
                            } finally {
                                Thread.currentThread().setName(name);
                            }

                        }
                    });
                    index = end;// 移动到下一批次
                }

                // 等待所有结果返回
                template.waitForResult();
            } finally {
                template.clear();
            }
        } else {
            doApply(records);
        }
    }

    public void setThreadSize(int threadSize) {
        this.threadSize = threadSize;
    }

    public void setSplitSize(int splitSize) {
        this.splitSize = splitSize;
    }
}

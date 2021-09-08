package com.gauss.controller;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.gauss.common.model.PrepareStatus;
import com.gauss.comparer.RecordComparer;
import com.gauss.preparer.GaussRecordPreparer;
import com.gauss.applier.RecordApplier;
import com.gauss.common.GaussConstants;
import com.gauss.common.audit.RecordDumper;
import com.gauss.common.lifecycle.AbstractGaussLifeCycle;
import com.gauss.common.model.DbType;
import com.gauss.common.model.ExtractStatus;
import com.gauss.common.model.ProgressStatus;
import com.gauss.common.model.GaussContext;
import com.gauss.common.stats.ProgressTracer;
import com.gauss.common.stats.StatAggregation;
import com.gauss.common.stats.StatAggregation.AggregationItem;
import com.gauss.common.utils.GaussUtils;
import com.gauss.common.utils.thread.NamedThreadFactory;
import com.gauss.common.utils.thread.GaussUncaughtExceptionHandler;
import com.gauss.exception.GaussException;
import com.gauss.extractor.RecordExtractor;

/**
 * 代表一个校验任务
 */
public class GaussInstance extends AbstractGaussLifeCycle {

    private final Logger logger = LoggerFactory.getLogger(GaussInstance.class);

    private GaussContext context;

    private GaussRecordPreparer preparer;

    private RecordExtractor extractor;

    private RecordApplier applier;

    private RecordComparer comparer;

    private TableController tableController;

    private ProgressTracer progressTracer;

    private StatAggregation statAggregation;

    private DbType targetDbType;

    private Thread worker = null;

    private CountDownLatch mutex = new CountDownLatch(1);

    private GaussException exception = null;

    private String tableShitKey;

    private int retryTimes = 1;

    private int retryInterval;

    private int noUpdateTimes = 0;

    private boolean concurrent = true;

    private int threadSize = 5;

    private ThreadPoolExecutor executor;

    private String executorName;

    public GaussInstance(GaussContext context) {
        this.context = context;
        this.tableShitKey = context.getTableMeta().getFullName();
    }

    public void start() {
        MDC.put(GaussConstants.MDC_TABLE_SHIT_KEY, tableShitKey);
        super.start();

        try {
            tableController.acquire();

            executorName = this.getClass().getSimpleName() + "-" + context.getTableMeta().getFullName();
            if (executor == null) {
                executor = new ThreadPoolExecutor(threadSize, threadSize, 60, TimeUnit.SECONDS,
                    new ArrayBlockingQueue(threadSize * 2), new NamedThreadFactory(executorName),
                    new ThreadPoolExecutor.CallerRunsPolicy());
            }
            if (!preparer.isStart()) {
                preparer.start();
            }
            if (!extractor.isStart()) {
                extractor.start();
            }
            if (!applier.isStart()) {
                applier.start();
            }

            worker = new Thread(new Runnable() {

                public void run() {
                    try {
                        for (int i = 0; i < retryTimes; i++) {
                            MDC.remove(GaussConstants.MDC_TABLE_SHIT_KEY);
                            if (i > 0) {
                                logger.info("table[{}] is start , retrying ", context.getTableMeta().getFullName());
                            } else {
                                logger.info("table[{}] is start", context.getTableMeta().getFullName());
                            }

                            try {
                                processTable();
                                exception = null;
                                break;
                            } catch (GaussException e) {
                                exception = e;
                                if (processException(e, i)) {
                                    break;
                                }
                                GaussUtils.outputUnnormal(context.getTableMeta().getFullName());
                            } finally {
                                MDC.remove(GaussConstants.MDC_TABLE_SHIT_KEY);
                            }
                        }

                        if (exception == null) {
                            // 记录到总文件下
                            logger.info("table[{}] is end", context.getTableMeta().getFullName());
                        } else if (ExceptionUtils.getRootCause(exception) instanceof InterruptedException) {
                            progressTracer.update(context.getTableMeta().getFullName(), ProgressStatus.FAILED);
                            logger.info("table[{}] is interrpt ,current status:{} !",
                                context.getTableMeta().getFullName(), extractor.getStatus());
                        } else {
                            progressTracer.update(context.getTableMeta().getFullName(), ProgressStatus.FAILED);
                            logger.info("table[{}] is error , current status:{} !",
                                context.getTableMeta().getFullName(), extractor.getStatus());
                        }
                    } finally {
                        tableController.release(GaussInstance.this);
                        // 标记为成功
                        mutex.countDown();
                    }

                }

                private void processTable() {
                    try {
                        MDC.put(GaussConstants.MDC_TABLE_SHIT_KEY, tableShitKey);
                        ExtractStatus status = ExtractStatus.NORMAL;
                        AtomicLong batchId = new AtomicLong(0);
                        long tpsLimit = context.getTpsLimit();
                        do {
                            long start = System.currentTimeMillis();
                            // extract checksum
                            List<String> records = extractor.extract();
                            if (GaussUtils.isEmpty(records)) {
                                status = extractor.getStatus();
                            }

                            RecordDumper.dumpExtractorInfo(batchId.incrementAndGet(), records);

                            // insert checksum into target database
                            Throwable applierException = null;
                            for (int i = 0; i < retryTimes; i++) {
                                try {
                                    applier.apply(records);
                                    applierException = null;
                                    break;
                                } catch (Throwable e) {
                                    applierException = e;
                                    if (processException(e, i)) {
                                        break;
                                    }
                                }
                            }

                            if (applierException != null) {
                                throw applierException;
                            }

                            RecordDumper.dumpApplierInfo(batchId.get(), records, records);

                            long end = System.currentTimeMillis();

                            if (tpsLimit > 0) {
                                tpsControl(records, start, end, tpsLimit);
                                end = System.currentTimeMillis();
                            }

                            if (GaussUtils.isNotEmpty(records)) {
                                statAggregation.push(new AggregationItem(start, end, Long.valueOf(records.size())));
                            }

                        } while (status != ExtractStatus.TABLE_END);
                        while (true) {
                            if (preparer.getStatus() == PrepareStatus.END) {
                                comparer.compare();
                                break;
                            }
                            Thread.sleep(100);
                        }
                        logger.info("table[{}] is end by {}", context.getTableMeta().getFullName(), status);
                        statAggregation.print();
                    } catch (InterruptedException e) {
                        throw new GaussException(e);
                    } catch (Throwable e) {
                        throw new GaussException(e);
                    }
                }

                private boolean processException(Throwable e, int i) {
                    if (!(ExceptionUtils.getRootCause(e) instanceof InterruptedException)) {
                        logger.error("retry {} ,something error happened. caused by {}", (i + 1),
                            ExceptionUtils.getFullStackTrace(e));
                        try {
                            Thread.sleep(retryInterval);
                        } catch (InterruptedException e1) {
                            exception = new GaussException(e1);
                            Thread.currentThread().interrupt();
                            return true;
                        }
                    } else {
                        // interrupt事件，响应退出
                        return true;
                    }

                    return false;
                }

            });

            worker.setUncaughtExceptionHandler(new GaussUncaughtExceptionHandler(logger));
            worker.setName(this.getClass().getSimpleName() + "-" + context.getTableMeta().getFullName());
            worker.start();

            logger.info("table[{}] start successful. extractor:{} , applier:{}", new Object[] {
                context.getTableMeta().getFullName(), extractor.getClass().getName(), applier.getClass().getName()
            });
        } catch (InterruptedException e) {
            progressTracer.update(context.getTableMeta().getFullName(), ProgressStatus.FAILED);
            exception = new GaussException(e);
            mutex.countDown();
            tableController.release(this); // 释放下
            Thread.currentThread().interrupt();
        } catch (Throwable e) {
            progressTracer.update(context.getTableMeta().getFullName(), ProgressStatus.FAILED);
            exception = new GaussException(e);
            mutex.countDown();
            logger.error("table[{}] start failed caused by {}", context.getTableMeta().getFullName(),
                ExceptionUtils.getFullStackTrace(e));
            tableController.release(this); // 释放下
        }
    }

    /**
     * 等待instance处理完成
     */
    public void waitForDone() throws InterruptedException, GaussException {
        mutex.await();

        if (exception != null) {
            throw exception;
        }
    }

    public void stop() {
        MDC.put(GaussConstants.MDC_TABLE_SHIT_KEY, tableShitKey);
        super.stop();

        // 尝试中断
        if (worker != null) {
            worker.interrupt();
            try {
                worker.join(2 * 1000);
            } catch (InterruptedException e) {
                // ignore
            }
        }

        if (preparer.isStart()) {
            preparer.stop();
        }

        if (extractor.isStart()) {
            extractor.stop();
        }

        executor.shutdownNow();

        exception = null;
        logger.info("table[{}] stop successful. ", context.getTableMeta().getFullName());
    }

    private void tpsControl(List<String> result, long start, long end, long tps) throws InterruptedException {
        long expectTime = (result.size() * 1000) / tps;
        long runTime = expectTime - (end - start);
        if (runTime > 0) {
            Thread.sleep(runTime);
        }
    }

    public RecordExtractor getExtractor() {
        return extractor;
    }

    public void setPreparer(GaussRecordPreparer preparer) {
        this.preparer = preparer;
    }

    public GaussRecordPreparer getPreparer() {
        return preparer;
    }

    public void setExtractor(RecordExtractor extractor) {
        this.extractor = extractor;
    }

    public RecordApplier getApplier() {
        return applier;
    }

    public void setApplier(RecordApplier applier) {
        this.applier = applier;
    }

    public void setComparer(RecordComparer comparer) {
        this.comparer = comparer;
    }

    public RecordComparer getComparer() {
        return comparer;
    }

    public void setTableController(TableController tableController) {
        this.tableController = tableController;
    }

    public void setTableShitKey(String tableShitKey) {
        this.tableShitKey = tableShitKey;
    }

    public void setStatAggregation(StatAggregation statAggregation) {
        this.statAggregation = statAggregation;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }

    public void setTargetDbType(DbType targetDbType) {
        this.targetDbType = targetDbType;
    }

    public GaussContext getContext() {
        return context;
    }

    public void setProgressTracer(ProgressTracer progressTracer) {
        this.progressTracer = progressTracer;
    }

    public void setThreadSize(int threadSize) {
        this.threadSize = threadSize;
    }

    public void setConcurrent(boolean concurrent) {
        this.concurrent = concurrent;
    }

    public void setExecutor(ThreadPoolExecutor executor) {
        this.executor = executor;
    }
}

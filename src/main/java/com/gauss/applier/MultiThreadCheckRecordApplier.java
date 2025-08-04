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

package com.gauss.applier;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.MDC;

import com.gauss.common.GaussConstants;
import com.gauss.common.model.GaussContext;
import com.gauss.common.utils.GaussUtils;
import com.gauss.common.utils.thread.ExecutorTemplate;
import com.gauss.common.utils.thread.NamedThreadFactory;
import com.gauss.exception.GaussException;

public class MultiThreadCheckRecordApplier extends CheckRecordApplier {

    private int                threadSize = 5;
    private int                splitSize  = 50;
    private ThreadPoolExecutor executor;
    private String             executorName;

    public MultiThreadCheckRecordApplier(GaussContext context, int threadSize, int splitSize,
                                         ThreadPoolExecutor executor, int query_dop){
        super(context,query_dop);

        this.threadSize = threadSize;
        this.splitSize = splitSize;
        this.executor = executor;
    }

    @Override
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

    public void apply(final List<String> records) throws GaussException {
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
                    final List<String> subList = records.subList(index, end);
                    template.submit(new Runnable() {

                        public void run() {
                            String name = Thread.currentThread().getName();
                            try {
                                MDC.put(GaussConstants.MDC_TABLE_SHIT_KEY, context.getTableMeta().getFullName());
                                Thread.currentThread().setName(executorName);
                                doApply(subList);
                            } catch (SQLException | IOException throwables) {
                                logger.error("## Something goes wrong when inserting checksum:\n{}",
                                    ExceptionUtils.getFullStackTrace(throwables));
                                System.exit(0);
                            } finally {
                                Thread.currentThread().setName(name);
                            }

                        }
                    });
                    index = end;// move to next batch
                }

                // waiting fot all results
                template.waitForResult();
            } finally {
                template.clear();
            }
        } else {
            try {
                doApply(records);
            } catch (IOException | SQLException e) {
                logger.error("## Something goes wrong when inserting checksum:\n{}",
                    ExceptionUtils.getFullStackTrace(e));
                System.exit(0);
            }
        }
    }
}

package com.gauss.common.utils.thread;

import org.slf4j.Logger;

public class GaussUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private Logger logger;

    public GaussUncaughtExceptionHandler(Logger logger){
        this.logger = logger;
    }

    public void uncaughtException(Thread t, Throwable e) {
        logger.error("uncaught exception", e);
    }
}

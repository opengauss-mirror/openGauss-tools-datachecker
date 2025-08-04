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

package com.gauss.common.utils.thread;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamedThreadFactory implements ThreadFactory {

    private static final Logger                   logger                   = LoggerFactory.getLogger(NamedThreadFactory.class);
    private final static UncaughtExceptionHandler uncaughtExceptionHandler = new GaussUncaughtExceptionHandler(logger);
    private final static String                   DEFAULT_NAME             = "Gauss";
    private String                                threadName;
    private AtomicInteger                         threadNumber             = new AtomicInteger(0);
    private boolean                               daemon                   = true;

    public NamedThreadFactory(){
        this(DEFAULT_NAME, true);
    }

    public NamedThreadFactory(String name){
        this(name, true);
    }

    public NamedThreadFactory(String name, boolean daemon){
        this.threadName = name;
        this.daemon = daemon;
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, threadName + "-" + threadNumber.getAndIncrement());
        t.setDaemon(daemon);
        t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return t;
    }

}

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

package com.gauss.common.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractGaussLifeCycle implements GaussLifeCycle {

    protected final Logger     logger  = LoggerFactory.getLogger(this.getClass());
    protected volatile boolean running = false;                                   // 是否处于运行中

    public boolean isStart() {
        return running;
    }

    public void start() {
        if (running) {
            return;
        }

        running = true;
    }

    public void stop() {
        if (!running) {
            return;
        }

        running = false;
    }

    public void abort(String why, Throwable e) {
        logger.error("abort caused by " + why, e);
        stop();
    }

    public boolean isStop() {
        return !isStart();
    }

}

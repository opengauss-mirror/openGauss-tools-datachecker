package com.gauss.common.lifecycle;

/**
 * 对应的lifecycle控制接口
 */
public interface GaussLifeCycle {

    public void start();

    public void stop();

    /**
     * 异常stop的机制
     */
    public void abort(String why, Throwable e);

    public boolean isStart();

    public boolean isStop();

}

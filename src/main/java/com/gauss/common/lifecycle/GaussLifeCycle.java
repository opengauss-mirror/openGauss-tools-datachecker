package com.gauss.common.lifecycle;

public interface GaussLifeCycle {

    public void start();

    public void stop();

    public void abort(String why, Throwable e);

    public boolean isStart();

    public boolean isStop();

}

package com.gauss.common.model;

/**
 * 运行模式
 */
public enum RunMode {
    /**
     * 对比
     */
    CHECK;

    public boolean isCheck() {
        return this == RunMode.CHECK;
    }
}

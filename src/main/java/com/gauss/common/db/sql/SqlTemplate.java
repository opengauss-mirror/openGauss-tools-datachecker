/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 */
package com.gauss.common.db.sql;

import java.util.ArrayList;

public abstract class SqlTemplate {
    /* special type */
    static final int BINARY_FLOAT = 100;

    static final int BINARY_DOUBLE = 101;

    static final int INTERVAL_YEAR = -103;

    static final int INTERVAL_DAY = -104;

    static final int BFILE = -13;

    public String getExtractSql() {
        return null;
    }
    public String getPrepareSql() {
        return null;
    }
    public String getCompareSql() {
        return null;
    }
    abstract public String getMd5Sql();
    abstract public String getSearchSql(ArrayList<String> md5list);
}

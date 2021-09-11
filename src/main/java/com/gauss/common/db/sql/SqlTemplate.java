/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 */
package com.gauss.common.db.sql;

import com.gauss.common.model.GaussContext;

import java.util.ArrayList;

public interface SqlTemplate {
    public String getMd5Sql();
    public String getExtractSql();
    public String getSearchSql(ArrayList<String> md5list);
    public String getPrepareSql();
    public String getCompareSql();
}

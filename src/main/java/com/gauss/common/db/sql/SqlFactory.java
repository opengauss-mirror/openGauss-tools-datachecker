/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 */
package com.gauss.common.db.sql;

import com.gauss.common.model.DbType;
import com.gauss.common.model.GaussContext;
import com.gauss.exception.GaussException;

public class SqlFactory {
    public SqlTemplate getSqlTemplate(DbType dbType, GaussContext context) {
        if (dbType.isMysql()) {
            return new MysqlUtil(context);
        } else if (dbType.isOracle()) {
            return new OracleUtil(context);
        } else if (dbType.isOpenGauss()) {
            return new OpenGaussUtil(context);
        }
        throw new GaussException("Unkonwn database type");
    }
}

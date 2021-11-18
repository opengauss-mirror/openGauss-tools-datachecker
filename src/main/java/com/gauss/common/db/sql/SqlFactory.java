/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 */
package com.gauss.common.db.sql;

import com.gauss.common.model.DbType;
import com.gauss.common.model.GaussContext;
import com.gauss.exception.GaussException;

public class SqlFactory {
    public SqlTemplate getSqlTemplate(DbType type, DbType srcType, GaussContext context) {
        if (type.isMysql()) {
            return new MysqlUtil(context);
        } else if (type.isOracle()) {
            return new OracleUtil(context); 
        } else if (type.isPostgreSQL()) {
            return new PostgresUtil(context);
        } else if (type.isOpenGauss()) {
            if (srcType.isOracle()) {
                return new OpenGaussForOracle(context);
            } else if (srcType.isMysql()) {
                return new OpenGaussForMysql(context);
            } else if (srcType.isPostgreSQL()) {
                return new OpenGaussForPostgreSQL(context);
            }
        }
        throw new GaussException("Unknown database type");
    }
}

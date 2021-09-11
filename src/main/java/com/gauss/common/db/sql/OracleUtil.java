/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 */

package com.gauss.common.db.sql;
import com.gauss.common.db.meta.ColumnMeta;
import com.gauss.common.db.meta.Table;
import com.gauss.common.model.GaussContext;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OracleUtil implements SqlTemplate {
    private String orinTableName;

    private GaussContext context;

    static final String convertFloat = "round(%s, 10)";

    public OracleUtil(GaussContext context) {
        this.context = context;
        Table tableMeta = context.getTableMeta();
        this.orinTableName = tableMeta.getSchema() + ".\"" + tableMeta.getName() + "\"";
    }

    public String getMd5Sql() {
        StringBuilder sb = new StringBuilder();
        sb.append("lower(utl_raw.cast_to_raw(dbms_obfuscation_toolkit.md5(input_string=>");
        List<ColumnMeta> columns = context.getTableMeta().getColumns();
        for (ColumnMeta meta : columns) {
            switch (meta.getType()) {
                case Types.FLOAT:
                case Types.DOUBLE:
                    sb.append(String.format(convertFloat, meta.getName()));
                    break;
                default:
                    sb.append(meta.getName());
                    break;
            }
            sb.append(" || ");
        }
        int length = sb.length();
        sb.delete(length - 4, length);
        sb.append("))) ");
        return sb.toString();
    }

    public String getExtractSql() {
        return "select " + getMd5Sql() + " from " + orinTableName;
    }

    public String getSearchSql(ArrayList<String> md5list) {
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(orinTableName).append(" where ").append(getMd5Sql());
        sb.append(" in (");
        sb.append(md5list.stream().map(str->"'" + str + "'").collect(Collectors.joining(",")));
        sb.append(")");
        return sb.toString();
    }

    public String getPrepareSql() {
        return null;
    }

    public String getCompareSql() {
        return null;
    }
}

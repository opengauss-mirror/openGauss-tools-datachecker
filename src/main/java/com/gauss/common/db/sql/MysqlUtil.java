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

public class MysqlUtil implements SqlTemplate {
    private String orinTableName;

    private GaussContext context;

    static final String convertChar = "convert(`%s`, char)";

    static final String convertFloat = "round(convert(`%s`, char), 10)";

    static final String convertBit = "`%s`+0";

    static final String convertGeo = "substring(AsText(`%s`), 6)";

    static final String convertVarchar = "lower(hex(trim(TRAILING '\\0' from `%s`)))";

    static final String convertDefault = "`%s`";

    public MysqlUtil(GaussContext context) {
        this.context = context;
        Table tableMeta = context.getTableMeta();
        this.orinTableName = "`" + tableMeta.getSchema() + "`.`" + tableMeta.getName() + "`";
    }

    public String getMd5Sql() {
        StringBuilder sb = new StringBuilder();
        sb.append("md5(concat_ws('',");
        List<ColumnMeta> columns = context.getTableMeta().getColumns();
        for (ColumnMeta meta : columns) {
            switch (meta.getType()) {
                case Types.BOOLEAN:
                case Types.CHAR:
                    sb.append(String.format(convertChar, meta.getName()));
                    break;
                case Types.REAL:
                case Types.FLOAT:
                case Types.DOUBLE:
                    sb.append(String.format(convertFloat, meta.getName()));
                    break;
                case Types.BIT:
                    sb.append(String.format(convertBit, meta.getName()));
                    break;
                case Types.VARBINARY:
                case Types.BINARY:
                case Types.LONGVARBINARY:
                    if (meta.getTypeName().equals("GEOMETRY")) {
                        sb.append(String.format(convertGeo, meta.getName()));
                    } else {
                        sb.append(String.format(convertVarchar, meta.getName()));
                    }
                    break;
                default:
                    sb.append(String.format(convertDefault, meta.getName()));
                    break;
            }
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("))");
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

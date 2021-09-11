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

public class OpenGaussUtil implements SqlTemplate {
    private String compareTableName;

    private String orinTableName;

    private GaussContext context;

    static final String convertBit = "cast(\"%s\" as int)";

    static final String convertChar = "cast(\"%s\" as varchar)";

    static final String convertFloat = "round(\"%s\"::numeric, 10)";

    static final String convertGeo = "replace(cast(\"%s\" as varchar),',',' ')";

    static final String convertVarchar = "substring(cast(\"%s\" as varchar) from 3)";

    static final String convertDefault = "\"%s\"";

    public OpenGaussUtil(GaussContext context) {
        this.context = context;
        Table tableMeta = context.getTableMeta();
        this.orinTableName = "\"" + tableMeta.getSchema() + "\".\"" + tableMeta.getName() + "\"";
        this.compareTableName = "\"" + tableMeta.getSchema() + "\".\"" + tableMeta.getName() + "_dataChecker";
    }

    public String getMd5Sql() {
        StringBuilder sb = new StringBuilder();
        sb.append("md5(");
        List<ColumnMeta> columns = context.getTableMeta().getColumns();
        for (ColumnMeta meta : columns) {
            switch (meta.getType()) {
                case Types.BOOLEAN:
                case Types.BIT:
                    sb.append(String.format(convertBit, meta.getName()));
                    break;
                case Types.CHAR:
                    sb.append(String.format(convertChar, meta.getName()));
                    break;
                case Types.REAL:
                    //same as double
                case Types.FLOAT:
                    //same as double
                case Types.DOUBLE:
                    sb.append(String.format(convertFloat, meta.getName()));
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
            sb.append(" || ");
        }
        int length = sb.length();
        sb.delete(length - 4, length);
        sb.append(")");
        return sb.toString();
    }

    public String getExtractSql() {
        return null;
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
        String res = "insert into " + compareTableName + "B\" select " + getMd5Sql() + " from " + orinTableName + ";";
        return res;
    }

    public String getCompareSql() {
        return "select * from " + compareTableName + "A\" t1 full join " + compareTableName
                + "B\" t2 on t1.checksumA=t2.checksumB where t2.checksumB is null or t1.checksumA is null;";
    }
}

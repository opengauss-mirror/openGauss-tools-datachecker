/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 */
package com.gauss.common.db.sql;

import com.gauss.common.db.meta.ColumnMeta;
import com.gauss.common.db.meta.Table;
import com.gauss.common.model.GaussContext;
import com.gauss.common.utils.Quote;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MysqlUtil extends SqlTemplate {
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
        this.orinTableName = Quote.join("", "`", tableMeta.getSchema(), "`.`", tableMeta.getName(), "`");
    }

    static private String convert(ColumnMeta meta) {
        String columnName = meta.getName();
        switch(meta.getType()) {
            case Types.BOOLEAN:
            case Types.CHAR:
                return String.format(convertChar, columnName);
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return String.format(convertFloat, columnName);
            case Types.BIT:
                return String.format(convertBit, columnName);
            case Types.VARBINARY:
            case Types.BINARY:
            case Types.LONGVARBINARY:
                if (meta.getTypeName().equals("GEOMETRY")) {
                    return String.format(convertGeo, columnName);
                } else {
                    return String.format(convertVarchar, columnName);
                }
            default:
                return String.format(convertDefault, columnName);
        }
    }

    @Override
    public String getMd5Sql() {
        StringBuilder sb = new StringBuilder();
        sb.append("md5(concat_ws('',");
        List<ColumnMeta> columns = context.getTableMeta().getColumns();
        sb.append(columns.stream().map(MysqlUtil::convert).collect(Collectors.joining(",")));
        sb.append("))");
        return sb.toString();
    }

    @Override
    public String getExtractSql() {
        return Quote.join(" ","select", getMd5Sql(), "from", orinTableName);
    }

    @Override
    public String getSearchSql(ArrayList<String> md5list) {
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(orinTableName).append(" where ").append(getMd5Sql());
        sb.append(" in (");
        sb.append(md5list.stream().map(str->"'" + str + "'").collect(Collectors.joining(",")));
        sb.append(")");
        return sb.toString();
    }
}

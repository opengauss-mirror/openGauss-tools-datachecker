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

public class PostgresUtil extends SqlTemplate {

    private String orinTableName;

    private GaussContext context;

    static final String convertBit = "cast(%s as int)";

    static final String convertChar = "cast(%s as varchar)";

    static final String convertFloat = "round(%s::numeric, 10)";

    static final String convertGeo = "replace(cast(%s as varchar),',',' ')";

    static final String convertVarchar = "substring(cast(%s as varchar) from 3)";

    static final String convertIntervalDay = "round((extract(day from %s) * 60 * 60 * 24 + extract(hour from %s)" +
            " * 60 * 60 + extract(min from %s) * 60 + extract(second from %s))::numeric, 10)";

    static final String convertIntervalYear = "extract(year from %s) * 12 + extract(month from %s)";

    static final String convertDefault = "%s";

    public PostgresUtil(GaussContext context) {
        this.context = context;
        Table tableMeta = context.getTableMeta();
        this.orinTableName =  Quote.join("", tableMeta.getSchema(), ".\"", tableMeta.getName(), "\"");
    }

    static private String convert(ColumnMeta meta) {
        String columnName = meta.getName();
        switch(meta.getType()) {
            case Types.BOOLEAN:
            case Types.BIT:
                return String.format(convertBit, columnName);
            case Types.CHAR:
            case Types.ROWID:
            case Types.SQLXML:
                return String.format(convertChar, columnName);
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.NUMERIC:
                return String.format(convertFloat, columnName);
            case Types.VARBINARY:
            case Types.BINARY:
            case Types.LONGVARBINARY:
                if (meta.getTypeName().equals("GEOMETRY")) {
                    return String.format(convertGeo, columnName);
                } else {
                    return String.format(convertVarchar, columnName);
                }
            case INTERVAL_DAY:
                return String.format(convertIntervalDay, columnName, columnName, columnName, columnName);
            case INTERVAL_YEAR:
                return String.format(convertIntervalYear, columnName, columnName);
            default:
                return String.format(convertDefault, columnName);
        }
    }

    @Override
    public String getMd5Sql() {
        StringBuilder sb = new StringBuilder();
        sb.append("md5(concat_ws('',");
        List<ColumnMeta> columns = context.getTableMeta().getColumns();
        sb.append(columns.stream().map(PostgresUtil::convert).collect(Collectors.joining(",")));
        sb.append("))");
        return sb.toString();
    }

    @Override
    public String getExtractSql() {
        return Quote.join(" ","select ", getMd5Sql(), "from", orinTableName);
    }

    @Override
    public String getSearchSql(ArrayList<String> md5list) {
        return Quote.join(" ", "select * from", orinTableName, "where", getMd5Sql(), "in (",
                md5list.stream().map(str->"'" + str + "'").collect(Collectors.joining(",")), ")");
    }
}

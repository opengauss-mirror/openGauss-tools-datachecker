/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 */
package com.gauss.common.db.sql;

import com.gauss.common.db.meta.ColumnMeta;
import com.gauss.common.model.GaussContext;
import com.gauss.common.utils.Quote;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class OpenGaussUtil extends SqlTemplate {
    private String srcCompareTableName;

    private String destCompareTableName;

    private String orinTableName;

    private GaussContext context;

    private String concatStart;

    private String concatEnd;

    private String delimiter;

    private String quote;

    public OpenGaussUtil(GaussContext context) {this.context = context;};

    static final String convertBit = "cast(%s as int)";

    static final String convertChar = "cast(%s as varchar)";

    static final String convertFloat = "round(%s::numeric, 10)";

    static final String convertGeo = "replace(cast(%s as varchar),',',' ')";

    static final String convertVarchar = "substring(cast(%s as varchar) from 3)";

    static final String convertDefault = "%s";

    static final String convertIntervalDay = "round((extract(day from %s) * 60 * 60 * 24 + extract(hour from %s)" +
            " * 60 * 60 + extract(min from %s) * 60 + extract(second from %s))::numeric, 10)";

    static final String convertIntervalYear = "extract(year from %s) * 12 + extract(month from %s)";

    private String convert(ColumnMeta meta) {
        String columnName = quote + meta.getName() + quote;
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

    public String getMd5Sql() {
        StringBuilder sb = new StringBuilder();
        sb.append("md5(");
        sb.append(concatStart);
        List<ColumnMeta> columns = context.getTableMeta().getColumns();
        sb.append(columns.stream().map(this::convert).collect(Collectors.joining(delimiter)));
        sb.append(concatEnd);
        sb.append(")");
        return sb.toString();
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
        return Quote.join(" ",
                "insert into", destCompareTableName, "select", getMd5Sql(), "from", orinTableName);
    }

    public String getCompareSql() {
        return Quote.join(" ",
                "select * from", srcCompareTableName, "t1 full join", destCompareTableName,
                "t2 on t1.checksumA=t2.checksumB where (t2.checksumB is null or t1.checksumA is null)",
                "and (t2.checksumB is not null or t1.checksumA is not null);");
    }

    public void setConcatStart(String concatStart) { this.concatStart = concatStart; }

    public void setConcatEnd(String concatEnd) { this.concatEnd = concatEnd; }

    public void setDelimiter(String delimiter) { this.delimiter = delimiter; }

    public void setQuote(String quote) { this.quote = quote; }

    public void setSrcCompareTableName(String srcCompareTableName) { this.srcCompareTableName = srcCompareTableName; }

    public void setDestCompareTableName(String destCompareTableName) { this.destCompareTableName = destCompareTableName; }

    public void setOrinTableName(String orinTableName) { this.orinTableName = orinTableName; }
}

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

public class OracleUtil extends SqlTemplate {
    private String orinTableName;

    private GaussContext context;

    static final int maxInValues = 999;

    static final String convertFloat = "to_char(\"%s\", 'fm99999999999999999999.0000000000')";

    static final String convertBlob = "UTL_RAW.CAST_TO_VARCHAR2(DBMS_LOB.SUBSTR(\"%s\"))";

    static final String convertBinFloat = "to_char(\"%s\") \"%s\"";

    static final String convertXml = "(\"%s\").getclobval()";

    static final String convertIntervalYear = "EXTRACT(YEAR FROM \"%s\") * 12 + EXTRACT(MONTH FROM \"%s\")";

    static final String convertIntervalDay = "to_char(EXTRACT(DAY FROM \"%s\") * 60 * 60 * 24" +
            " + EXTRACT(HOUR FROM \"%s\") * 60 * 60 + EXTRACT(MINUTE FROM \"%s\") * 60 + EXTRACT(SECOND FROM \"%s\")," +
            "'fm99999999999999999999.0000000000')";

    static final String convertTimestamp = "to_char(\"%s\", 'YYYY-MM-DD HH24:MI:SS')";

    static final String convertVarchar = "lower(rawtohex(\"%s\"))";

    static final String convertBfile = "datachecker_get_bfile(\"%s\")";

    static final String convertBfile2 = "datachecker_get_bfile(\"%s\") \"%s\"";

    static final String convertNchar = "rtrim(\"%s\")";

    static final String convertDefault = "\"%s\"";

    public OracleUtil(GaussContext context) {
        this.context = context;
        Table tableMeta = context.getTableMeta();
        this.orinTableName = Quote.join("", tableMeta.getSchema(), ".\"", tableMeta.getName(), "\"");
    }

    static private String convert(ColumnMeta meta) {
        String columnName = meta.getName();
        switch(meta.getType()) {
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.NUMERIC:
                return String.format(convertFloat, columnName);
            case Types.BLOB:
                return String.format(convertBlob, columnName);
            case Types.SQLXML:
                return String.format(convertXml, columnName);
            case Types.TIMESTAMP:
                return String.format(convertTimestamp, columnName);
            case Types.VARBINARY:
                return String.format(convertVarchar, columnName);
            case Types.NCHAR:
            case Types.CHAR:
                return String.format(convertNchar, columnName);
            case BFILE:
                return String.format(convertBfile, columnName);
            case INTERVAL_YEAR:
                return String.format(convertIntervalYear, columnName, columnName);
            case INTERVAL_DAY:
                return String.format(convertIntervalDay, columnName, columnName, columnName, columnName);
            default:
                return String.format(convertDefault, columnName);
        }
    }

    static private String convertSearch(ColumnMeta meta) {
        /*
         * Some columns type is not a valid sql_type in java.sql.Types, so if we do select * from xxx,
         * it will failed since jdbctemplate can't recognize the results type. Change those columns results
         * to a valid sql_type.
         */
        String columnName = meta.getName();
        switch(meta.getType()) {
            case BINARY_FLOAT:
            case BINARY_DOUBLE:
            case INTERVAL_DAY:
            case INTERVAL_YEAR:
                return String.format(convertBinFloat, columnName, columnName);
            case BFILE:
                return String.format(convertBfile2, columnName, columnName);
            default:
                return String.format(convertDefault, columnName);
        }
    }

    @Override
    public String getMd5Sql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DATACHECKER_MD5(");
        List<ColumnMeta> columns = context.getTableMeta().getColumns();
        sb.append(columns.stream().map(OracleUtil::convert).collect(Collectors.joining(" || ")));
        sb.append(") ");
        return sb.toString();
    }

    @Override
    public String getExtractSql() {
        return Quote.join(" ", "select", getMd5Sql(), "from", orinTableName);
    }

    @Override
    public String getSearchSql(ArrayList<String> md5list) {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        List<ColumnMeta> columns = context.getTableMeta().getColumns();
        sb.append(columns.stream().map(OracleUtil::convertSearch).collect(Collectors.joining(",")));
        sb.append(" from ").append(orinTableName).append(" where ");
        int size = md5list.size();
        for (int i = 0; i < size; i += maxInValues) {
            if (i != 0) {
                sb.append(" or ");
            }
            int toIndex = Math.min(i + maxInValues, size);
            sb.append(getMd5Sql()).append(" in (");
            sb.append(md5list.subList(i, toIndex).stream().map(str->"'" + str + "'").collect(Collectors.joining(",")));
            sb.append(")");
        }

        return sb.toString();
    }
}

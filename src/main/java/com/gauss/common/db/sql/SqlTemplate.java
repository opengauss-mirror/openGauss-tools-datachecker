package com.gauss.common.db.sql;

import java.util.List;

import com.gauss.common.db.meta.ColumnMeta;

/**
 * sql构造
 */
public class SqlTemplate {

    private static final String DOT = ".";

    /**
     * 根据字段的列表顺序，拼写以 col1,col2,col3,....
     */
    public String makeColumn(List<ColumnMeta> columns) {
        StringBuilder str = new StringBuilder();
        int size = columns.size();
        for (int i = 0; i < size; i++) {
            str.append(getColumnName(columns.get(i)));
            if (i < (size - 1)) {
                str.append(",");
            }
        }
        return str.toString();
    }

    /**
     * 根据字段列表，拼写column >= ? and column < ?
     */
    public String makeRange(ColumnMeta column) {
        return makeRange(column.getName());
    }

    /**
     * 根据字段列表，拼写 column >= ? and column < ?
     */
    public String makeRange(String columnName) {
        StringBuilder sb = new StringBuilder("");
        sb.append(getColumnName(columnName));
        sb.append(" >= ? and ");
        sb.append(getColumnName(columnName));
        sb.append(" <= ?");
        return sb.toString();
    }

    /**
     * 根据字段名和参数个数，拼写 column in (?,?,...) 字符串
     */
    public String makeIn(ColumnMeta column, int size) {
        return makeIn(column.getName(), size);
    }

    /**
     * 根据字段名和参数个数，拼写 column in (?,?,...) 字符串
     */
    public String makeIn(String columnName, int size) {
        StringBuilder sb = new StringBuilder("");
        sb.append(getColumnName(columnName));
        sb.append(" in (");
        for (int i = 0; i < size; i++) {
            sb.append("?");
            if (i != (size - 1)) {
                sb.append(",");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    public String getSelectSql(String schemaName, String tableName, String[] pkNames, String[] colNames) {
        StringBuilder sql = new StringBuilder();
        sql.append("select ");
        String[] allColumns = buildAllColumns(pkNames, colNames);
        int size = allColumns.length;
        for (int i = 0; i < size; i++) {
            sql.append(getColumnName(allColumns[i])).append(splitCommea(size, i));
        }

        sql.append(" from ").append(makeFullName(schemaName, tableName)).append(" where ( ");
        if (pkNames.length > 0) { // 可能没有主键
            makeColumnEquals(sql, pkNames, "and");
        } else {
            makeColumnEquals(sql, colNames, "and");
        }
        sql.append(" ) ");
        return sql.toString().intern();
    }

    protected String makeFullName(String schemaName, String tableName) {
        String full = schemaName + DOT + tableName;
        return full.intern();
    }

    protected void makeColumnEquals(StringBuilder sql, String[] columns, String separator) {
        int size = columns.length;
        for (int i = 0; i < size; i++) {
            sql.append(" ").append(getColumnName(columns[i])).append(" = ").append("? ");
            if (i != size - 1) {
                sql.append(separator);
            }
        }
    }

    protected String getColumnName(String columName) {
        return columName;
    }

    protected String getColumnName(ColumnMeta column) {
        return column.getName();
    }

    protected String splitCommea(int size, int i) {
        return (i + 1 < size) ? " , " : "";
    }

    protected String[] buildAllColumns(String[] pkNames, String[] colNames) {
        String[] allColumns = new String[pkNames.length + colNames.length];
        System.arraycopy(colNames, 0, allColumns, 0, colNames.length);
        System.arraycopy(pkNames, 0, allColumns, colNames.length, pkNames.length);
        return allColumns;
    }

}

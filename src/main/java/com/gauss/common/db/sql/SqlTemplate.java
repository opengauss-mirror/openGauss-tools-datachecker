package com.gauss.common.db.sql;

import com.gauss.common.db.meta.ColumnMeta;
import com.gauss.common.model.DbType;
import com.gauss.common.model.GaussContext;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * sql template
 */
public class SqlTemplate {
    private DbType dbType;

    private String orinTableName;

    private String CompareTableName;

    private GaussContext context;

    public SqlTemplate(DbType dbType, GaussContext context) {
        this.context = context;
        this.dbType = dbType;
        this.orinTableName = context.getTableMeta().getFullName();
        this.CompareTableName = orinTableName + "_dataChecker";
    }

    public String getOracleMd5() {
        StringBuilder sb = new StringBuilder();
        sb.append("lower(utl_raw.cast_to_raw(dbms_obfuscation_toolkit.md5(input_string=>");
        List<ColumnMeta> columns =  context.getTableMeta().getColumns();
        for (ColumnMeta meta : columns) {
            switch (meta.getType()) {
                case Types.FLOAT:
                case Types.DOUBLE:
                    sb.append("round(").append(meta.getName()).append(", 10)");
                    break;
                default:
                    sb.append(meta.getName());
            }
            sb.append(" || '_' || ");
        }
        int length = sb.length();
        sb.delete(length-11,length);
        sb.append("))) ");
        return sb.toString();
    }

    public String getMysqlMd5() {
        StringBuilder sb = new StringBuilder();
        sb.append("md5(concat_ws('_',");
        List<ColumnMeta> columns = context.getTableMeta().getColumns();
        for (ColumnMeta meta : columns) {
            switch (meta.getType()) {
                case Types.FLOAT:
                case Types.DOUBLE:
                    sb.append("round(").append(meta.getName()).append(", 10)");
                    break;
                case Types.BINARY:
                    sb.append("lower(hex(").append(meta.getName()).append("))");
                    break;
                default:
                    sb.append(meta.getName());
            }
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("))");
        return sb.toString();
    }

    public String getOpgsMd5() {
        StringBuilder sb = new StringBuilder();
        sb.append("md5(");
        List<ColumnMeta> columns = context.getTableMeta().getColumns();
        for (ColumnMeta meta : columns) {
            switch (meta.getType()) {
                case Types.BOOLEAN:
                    sb.append("cast(").append(meta.getName()).append(" as int");
                    break;
                case Types.FLOAT:
                    //same as double
                case Types.DOUBLE:
                    sb.append("round(").append(meta.getName()).append("::numeric, 10)");
                    break;
                case Types.BINARY:
                    sb.append("substring(cast(").append(meta.getName()).append(" as varchar) from 3)");
                    break;
                default:
                    sb.append(meta.getName());
            }
            sb.append(" || '_' || ");
        }
        int length = sb.length();
        sb.delete(length-11,length);
        sb.append(")");
        return sb.toString();
    }

    public String getPrepareSql() {
        String res = "insert into " + CompareTableName + "B select " + getOpgsMd5() + " from " + orinTableName + ";";
        return res;
    }

    public String getExtractSql() {
        String res = "select ";
        if (dbType == DbType.MYSQL) {
            res += getMysqlMd5();
        } else if (dbType == DbType.ORACLE) {
            res += getOracleMd5();
        } else {
            //todo
            res = "";
        }
        res += " from " + orinTableName;
        return res;
    }

    public String getCompareSql() {
        return "select * from " + CompareTableName + "A t1 full join " + CompareTableName
            + "B t2 on t1.checksumA=t2.checksumB where t2.checksumB is null or t1.checksumA is null;";
    }

    public String getSearchSql(ArrayList<String> md5list) {
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(orinTableName).append(" where ");
        if (dbType == DbType.MYSQL) {
            sb.append(getMysqlMd5());
        } else if (dbType == DbType.ORACLE) {
            sb.append(getOracleMd5());
        } else {
            //todo
        }
        sb.append(" in (");
        for (String str : md5list) {
            sb.append("'").append(str).append("',");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        return sb.toString();
    }
}

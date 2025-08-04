/*
This program is free software;
you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program;
if not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
*/

package com.gauss.common.db.meta;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;

import com.gauss.common.model.DbType;
import com.gauss.common.utils.GaussUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import com.google.common.collect.Lists;
import com.gauss.common.utils.LikeUtil;
import com.gauss.exception.GaussException;


/**
 * gey table meta
 */
public class TableMetaGenerator {
    public static Table getTableMeta(final DataSource dataSource, final String schemaName, final String tableName) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        return (Table) jdbcTemplate.execute(new ConnectionCallback() {

            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet rs = null;
                rs = metaData.getTables(schemaName, schemaName, tableName, new String[] { "TABLE" });
                Table table = null;
                while (rs.next()) {
                    String catlog = rs.getString(1);
                    String schema = rs.getString(2);
                    String name = rs.getString(3);
                    String type = rs.getString(4);

                    if ((schemaName == null || LikeUtil.isMatch(schemaName, catlog) || LikeUtil.isMatch(schemaName, schema))
                        && LikeUtil.isMatch(tableName, name)) {
                        table = new Table(type, StringUtils.isEmpty(catlog) ? schema : catlog, name);
                        break;
                    }
                }
                rs.close();

                if (table == null) {
                    throw new GaussException("table[" + schemaName + "." + tableName + "] is not found");
                }

                // get all columns
                rs = metaData.getColumns(schemaName, schemaName, tableName, null);
                List<ColumnMeta> columnList = new ArrayList<ColumnMeta>();
                while (rs.next()) {
                    String catlog = rs.getString(1);
                    String schema = rs.getString(2);
                    String name = rs.getString(3);
                    if ((schemaName == null || LikeUtil.isMatch(schemaName, catlog) || LikeUtil.isMatch(schemaName, schema))
                        && LikeUtil.isMatch(tableName, name)) {
                        String columnName = rs.getString(4); // COLUMN_NAME
                        int columnType = rs.getInt(5);
                        String typeName = rs.getString(6);
                        columnType = convertSqlType(columnType, typeName);
                        ColumnMeta col = new ColumnMeta(columnName, columnType, typeName);
                        columnList.add(col);
                    }
                }
                rs.close();

                table.getColumns().addAll(columnList);
                return table;
            }

        });
    }

    /**
     * get Table Metas Without Column
     */
    @SuppressWarnings("unchecked")
    public static List<Table> getTableMetasWithoutColumn(final DataSource dataSource, final String schemaName,
                                                         final String tableName) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        return (List<Table>) jdbcTemplate.execute((ConnectionCallback) conn -> {
            DatabaseMetaData metaData = conn.getMetaData();
            List<Table> result = Lists.newArrayList();
            String databaseName = metaData.getDatabaseProductName();
            ResultSet rs;
            Table table;
            Statement stmt = conn.createStatement();
            StringBuffer query;
            if (StringUtils.startsWithIgnoreCase(databaseName, "oracle")) {
                //Oracle
                if (StringUtils.isEmpty(tableName)) {
                    // ignore system tales
                    query = new StringBuffer("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS SCHEMA_NAME , TABLE_NAME" +
                            " FROM USER_TABLES T , USER_USERS U WHERE U.USERNAME = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')" +
                            " AND (T.IOT_TYPE != 'IOT_OVERFLOW' OR T.IOT_TYPE IS NULL)");
                } else {
                    query = new StringBuffer("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS SCHEMA_NAME , TABLE_NAME" +
                            " FROM USER_TABLES T , USER_USERS U WHERE T.TABLE_NAME ='" + tableName.toUpperCase() + "'");
                }
            } else {
                //Mysql and PostgreSQL
                if (StringUtils.isEmpty(tableName)) {
                    // ignore system tales
                    query = new StringBuffer("select TABLE_SCHEMA, TABLE_NAME from information_schema.tables where table_schema='"+ schemaName
                        +"' and table_type= 'BASE TABLE'");
                } else {
                    query = new StringBuffer("select TABLE_SCHEMA, TABLE_NAME from information_schema.tables where table_schema='"+ schemaName
                        +"' and table_name='"+ tableName +"'");
                }
            }
            rs = stmt.executeQuery(query.toString());
            while (rs.next()) {
                    String schema = rs.getString(1);
                    String tableName1 = rs.getString(2);
                    table = new Table("TABLE", schema, tableName1);
                    result.add(table);
            }
            rs.close();
            stmt.close();
            return result;
        });
    }



    public static void buildColumns(DataSource dataSource, final Table table) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute(new ConnectionCallback() {

            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = conn.getMetaData();
                DbType type = GaussUtils.judgeDbType(metaData.getDatabaseProductName());
                ResultSet rs;
                // get all columns
                rs = metaData.getColumns(type != DbType.MYSQL ? null : table.getSchema(), table.getSchema(),
                        table.getName(), null);

                List<ColumnMeta> columnList = new ArrayList<ColumnMeta>();
                while (rs.next()) {
                    String catlog = rs.getString(1);
                    String schema = rs.getString(2);
                    String name = rs.getString(3);
                    if ((table.getSchema() == null || LikeUtil.isMatch(table.getSchema(), catlog) || LikeUtil.isMatch(table.getSchema(),
                        schema)) && LikeUtil.isMatch(table.getName(), name)) {
                        String columnName = rs.getString(4); // COLUMN_NAME
                        int columnType = rs.getInt(5);
                        String typeName = rs.getString(6);
                        columnType = convertSqlType(columnType, typeName);
                        ColumnMeta col = new ColumnMeta(columnName, columnType, typeName);
                        columnList.add(col);
                    }
                }
                rs.close();
                table.getColumns().addAll(columnList);
                return null;
            }
        });

    }

    private static int convertSqlType(int columnType, String typeName) {
        String[] typeSplit = typeName.split(" ");
        if (typeSplit.length > 1) {
            if (columnType == Types.INTEGER && StringUtils.equalsIgnoreCase(typeSplit[1], "UNSIGNED")) {
                columnType = Types.BIGINT;
            }
        }

        if (columnType == Types.OTHER) {
            if (StringUtils.equalsIgnoreCase(typeName, "NVARCHAR")
                || StringUtils.equalsIgnoreCase(typeName, "NVARCHAR2")) {
                columnType = Types.VARCHAR;
            }

            if (StringUtils.equalsIgnoreCase(typeName, "NCLOB")) {
                columnType = Types.CLOB;
            }

            if (StringUtils.startsWithIgnoreCase(typeName, "TIMESTAMP")) {
                columnType = Types.TIMESTAMP;
            }
        }
        return columnType;
    }

}

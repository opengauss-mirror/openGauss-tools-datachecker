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
                String sName = getIdentifierName(schemaName, metaData);
                String tName = getIdentifierName(tableName, metaData);

                ResultSet rs = null;
                rs = metaData.getTables(sName, sName, tName, new String[] { "TABLE" });
                Table table = null;
                while (rs.next()) {
                    String catlog = rs.getString(1);
                    String schema = rs.getString(2);
                    String name = rs.getString(3);
                    String type = rs.getString(4);

                    if ((sName == null || LikeUtil.isMatch(sName, catlog) || LikeUtil.isMatch(sName, schema))
                        && LikeUtil.isMatch(tName, name)) {
                        table = new Table(type, StringUtils.isEmpty(catlog) ? schema : catlog, name);
                        break;
                    }
                }
                rs.close();

                if (table == null) {
                    throw new GaussException("table[" + schemaName + "." + tableName + "] is not found");
                }

                // get all columns
                rs = metaData.getColumns(sName, sName, tName, null);
                List<ColumnMeta> columnList = new ArrayList<ColumnMeta>();
                while (rs.next()) {
                    String catlog = rs.getString(1);
                    String schema = rs.getString(2);
                    String name = rs.getString(3);
                    if ((sName == null || LikeUtil.isMatch(sName, catlog) || LikeUtil.isMatch(sName, schema))
                        && LikeUtil.isMatch(tName, name)) {
                        String columnName = rs.getString(4); // COLUMN_NAME
                        int columnType = rs.getInt(5);
                        String typeName = rs.getString(6);
                        columnType = convertSqlType(columnType, typeName);
                        ColumnMeta col = new ColumnMeta(columnName, columnType, typeName);
                        columnList.add(col);
                    }
                }
                rs.close();

                // get primary keys
                List<String> primaryKeys = new ArrayList<String>();
                rs = metaData.getPrimaryKeys(sName, sName, tName);
                while (rs.next()) {
                    String catlog = rs.getString(1);
                    String schema = rs.getString(2);
                    String name = rs.getString(3);
                    if ((sName == null || LikeUtil.isMatch(sName, catlog) || LikeUtil.isMatch(sName, schema))
                        && LikeUtil.isMatch(tName, name)) {
                        primaryKeys.add(StringUtils.upperCase(rs.getString(4)));
                    }
                }
                rs.close();

                List<String> uniqueKeys = new ArrayList<String>();
                if (primaryKeys.isEmpty()) {
                    String lastIndexName = null;
                    rs = metaData.getIndexInfo(sName, sName, tName, true, true);
                    while (rs.next()) {
                        String catlog = rs.getString(1);
                        String schema = rs.getString(2);
                        String name = rs.getString(3);
                        if ((sName == null || LikeUtil.isMatch(sName, catlog) || LikeUtil.isMatch(sName, schema))
                            && LikeUtil.isMatch(tName, name)) {
                            String indexName = StringUtils.upperCase(rs.getString(6));
                            if ("PRIMARY".equals(indexName)) {
                                continue;
                            }

                            if (lastIndexName == null) {
                                lastIndexName = indexName;
                            } else if (!lastIndexName.equals(indexName)) {
                                break;
                            }

                            uniqueKeys.add(StringUtils.upperCase(rs.getString(9)));
                        }
                    }
                    rs.close();

                    // 如果无主键，使用唯一键
                    primaryKeys.addAll(uniqueKeys);
                }

                Set<ColumnMeta> columns = new HashSet<ColumnMeta>();
                Set<ColumnMeta> pks = new HashSet<ColumnMeta>();
                for (ColumnMeta columnMeta : columnList) {
                    if (primaryKeys.contains(columnMeta.getName())) {
                        pks.add(columnMeta);
                    } else {
                        columns.add(columnMeta);
                    }
                }

                table.getColumns().addAll(columns);
                table.getPrimaryKeys().addAll(pks);
                return table;
            }

        });
    }

    /**
     * get Table Metas Without Column
     */
    public static List<Table> getTableMetasWithoutColumn(final DataSource dataSource, final String schemaName,
                                                         final String tableName) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        return (List<Table>) jdbcTemplate.execute(new ConnectionCallback() {

            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = conn.getMetaData();
                List<Table> result = Lists.newArrayList();
                String databaseName = metaData.getDatabaseProductName();
                String sName = getIdentifierName(schemaName, metaData);
                String tName = getIdentifierName(tableName, metaData);
                ResultSet rs = null;
                Table table = null;
                Statement stmt = conn.createStatement();
                StringBuffer query;
                if (StringUtils.startsWithIgnoreCase(databaseName, "oracle")) {
                    //Oracle
                    if (StringUtils.isEmpty(tableName)) {
                        // ignore system tales
                        query = new StringBuffer("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS SCHEMA_NAME , TABLE_NAME FROM USER_TABLES T , USER_USERS U WHERE U.USERNAME = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')");
                    } else {
                        query = new StringBuffer("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS SCHEMA_NAME , TABLE_NAME FROM USER_TABLES T , USER_USERS U WHERE T.TABLE_NAME ='" + tName.toUpperCase() + "'");
                    }
                } else {
                    //Mysql
                    if (StringUtils.isEmpty(tableName)) {
                        // ignore system tales
                        query = new StringBuffer("select TABLE_SCHEMA, TABLE_NAME from information_schema.tables where table_schema='"+sName+"' and table_type= 'BASE TABLE'");
                    } else {
                        query = new StringBuffer("select TABLE_SCHEMA, TABLE_NAME from information_schema.tables where table_schema='"+sName+"' and table_name='"+tName+"'");
                    }
                }
                rs = stmt.executeQuery(query.toString());
                while (rs.next()) {
                        String schema = rs.getString(1);
                        String tableName = rs.getString(2);
                        table = new Table("TABLE", schema, tableName);
                        result.add(table);
                }
                rs.close();
                stmt.close();
                return result;
            }

        });
    }



    public static void buildColumns(DataSource dataSource, final Table table) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute(new ConnectionCallback() {

            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet rs;
                // get all columns
                rs = metaData.getColumns(table.getSchema(), table.getSchema(), table.getName(), null);
                List<ColumnMeta> columnList = new ArrayList<ColumnMeta>();

                while (rs.next()) {
                    String catlog = rs.getString(1);
                    String schema = rs.getString(2);
                    String name = rs.getString(3);
                    if ((table.getSchema() == null || LikeUtil.isMatch(table.getSchema(), catlog) || LikeUtil.isMatch(table.getSchema(),
                        schema))
                        && LikeUtil.isMatch(table.getName(), name)) {
                        String columnName = rs.getString(4); // COLUMN_NAME
                        int columnType = rs.getInt(5);
                        String typeName = rs.getString(6);
                        columnType = convertSqlType(columnType, typeName);
                        ColumnMeta col = new ColumnMeta(columnName, columnType, typeName);
                        columnList.add(col);
                    }
                }
                rs.close();

                // get getPrimaryKeys
                rs = metaData.getPrimaryKeys(table.getSchema(), table.getSchema(), table.getName());
                List<String> primaryKeys = new ArrayList<String>();
                while (rs.next()) {
                    String catlog = rs.getString(1);
                    String schema = rs.getString(2);
                    String name = rs.getString(3);
                    if ((table.getSchema() == null || StringUtils.equalsIgnoreCase(catlog, table.getSchema()) || StringUtils.equalsIgnoreCase(schema,
                        table.getSchema()))
                        && StringUtils.equalsIgnoreCase(name, table.getName())) {
                        primaryKeys.add(rs.getString(4));
                    }
                }
                rs.close();

                Set<ColumnMeta> columns = new HashSet<ColumnMeta>();
                Set<ColumnMeta> pks = new HashSet<ColumnMeta>();
                for (ColumnMeta columnMeta : columnList) {
                    if (primaryKeys.contains(columnMeta.getName())) {
                        pks.add(columnMeta);
                    } else {
                        columns.add(columnMeta);
                    }
                }

                table.getColumns().addAll(columns);
                table.getPrimaryKeys().addAll(pks);
                return null;
            }

        });

    }

    /**
     * get Identifier Name
     */
    private static String getIdentifierName(String name, DatabaseMetaData metaData) throws SQLException {
        if (metaData.storesMixedCaseIdentifiers()) {
            return name;
        } else if (metaData.storesUpperCaseIdentifiers()) {
            return StringUtils.upperCase(name);
        } else if (metaData.storesLowerCaseIdentifiers()) {
            return StringUtils.lowerCase(name);
        } else {
            return name;
        }
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

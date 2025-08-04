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

package com.gauss.common.model.record;

import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.collect.Lists;
import com.gauss.common.db.meta.ColumnValue;
import com.gauss.common.utils.GaussToStringStyle;
import com.gauss.exception.GaussException;

/**
 * represent one Record
 */
public class Record {

    private String            schemaName;
    private String            tableName;
    private List<ColumnValue> primaryKeys;
    private List<ColumnValue> columns;

    public Record(){

    }

    public Record(String schemaName, String tableName, List<ColumnValue> columns){
        this(schemaName, tableName, Lists.newArrayList(), columns);
    }


    public Record(String schemaName, String tableName, List<ColumnValue> primaryKeys, List<ColumnValue> columns){
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.primaryKeys = primaryKeys;
        this.columns = columns;
    }

    public List<ColumnValue> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<ColumnValue> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public void addPrimaryKey(ColumnValue primaryKey) {
        if (getColumnByName(primaryKey.getColumn().getName(), true) != null) {
            throw new GaussException("dup column[" + primaryKey.getColumn().getName() + "]");
        }
        primaryKeys.add(primaryKey);
    }

    public List<ColumnValue> getColumns() {
        return columns;
    }

    public void addColumn(ColumnValue column) {
        if (getColumnByName(column.getColumn().getName(), true) != null) {
            throw new GaussException("dup column[" + column.getColumn().getName() + "]");
        }
        columns.add(column);
    }


    @Deprecated
    public ColumnValue getPrimaryKeyByName(String pkName) {
        return getPrimaryKeyByName(pkName, false);
    }

    @Deprecated
    public ColumnValue getPrimaryKeyByName(String pkName, boolean returnNullNotExist) {
        for (ColumnValue pk : primaryKeys) {
            if (pk.getColumn().getName().equalsIgnoreCase(pkName)) {
                return pk;
            }
        }

        if (returnNullNotExist) {
            return null;
        } else {
            throw new GaussException("not found column[" + pkName + "]");
        }
    }


    public ColumnValue getColumnByName(String columnName) {
        return getColumnByName(columnName, false);
    }

    public ColumnValue getColumnByName(String columnName, boolean returnNullNotExist) {
        for (ColumnValue column : columns) {
            if (column.getColumn().getName().equalsIgnoreCase(columnName)) {
                return column;
            }
        }

        for (ColumnValue pk : primaryKeys) {
            if (pk.getColumn().getName().equalsIgnoreCase(columnName)) {
                return pk;
            }
        }

        if (returnNullNotExist) {
            return null;
        } else {
            throw new GaussException("not found column[" + columnName + "]");
        }
    }

    public ColumnValue removeColumnByName(String columnName) {
        return removeColumnByName(columnName, false);
    }

    public ColumnValue removeColumnByName(String columnName, boolean returnNullNotExist) {
        ColumnValue remove = null;
        for (ColumnValue pk : primaryKeys) {
            if (pk.getColumn().getName().equalsIgnoreCase(columnName)) {
                remove = pk;
                break;
            }
        }

        if (remove != null && this.primaryKeys.remove(remove)) {
            return remove;
        } else {
            for (ColumnValue column : columns) {
                if (column.getColumn().getName().equalsIgnoreCase(columnName)) {
                    remove = column;
                    break;
                }
            }

            if (remove != null && this.columns.remove(remove)) {
                return remove;
            }
        }

        if (returnNullNotExist) {
            return null;
        } else {
            throw new GaussException("not found column[" + columnName + "] to remove");
        }
    }

    public void setColumns(List<ColumnValue> columns) {
        this.columns = columns;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Record clone() {
        Record record = new Record();
        record.setTableName(this.tableName);
        record.setSchemaName(this.schemaName);
        for (ColumnValue column : primaryKeys) {
            record.addPrimaryKey(column.clone());
        }

        for (ColumnValue column : columns) {
            record.addColumn(column.clone());
        }
        return record;
    }

    public void clone(Record record) {
        record.setTableName(this.tableName);
        record.setSchemaName(this.schemaName);
        for (ColumnValue column : primaryKeys) {
            record.addPrimaryKey(column.clone());
        }

        for (ColumnValue column : columns) {
            record.addColumn(column.clone());
        }
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columns == null) ? 0 : columns.hashCode());
        result = prime * result + ((primaryKeys == null) ? 0 : primaryKeys.hashCode());
        result = prime * result + ((schemaName == null) ? 0 : schemaName.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Record other = (Record) obj;
        if (columns == null) {
            if (other.columns != null) return false;
        } else if (!columns.equals(other.columns)) return false;
        if (primaryKeys == null) {
            if (other.primaryKeys != null) return false;
        } else if (!primaryKeys.equals(other.primaryKeys)) return false;
        if (schemaName == null) {
            if (other.schemaName != null) return false;
        } else if (!schemaName.equals(other.schemaName)) return false;
        if (tableName == null) {
            if (other.tableName != null) return false;
        } else if (!tableName.equals(other.tableName)) return false;
        return true;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, GaussToStringStyle.DEFAULT_STYLE);
    }

}

package com.gauss.common.db.meta;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.collect.Lists;
import com.gauss.common.utils.GaussToStringStyle;

/**
 * represent one table
 */
public class Table {

    private String           type;
    private String           schema;
    private String           name;
    private List<ColumnMeta> columns     = Lists.newArrayList();

    public Table(String type, String schema, String name){
        this.type = type;
        this.schema = schema;
        this.name = name;
    }

    public Table(String type, String schema, String name, List<ColumnMeta> primaryKeys, List<ColumnMeta> columns){
        this.type = type;
        this.schema = schema;
        this.name = name;
        this.columns = columns;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }

    public List<ColumnMeta> getColumns() {
        return columns;
    }

    /**
     * 返回schema.name
     */
    public String getFullName() {
        return schema + "." + name;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, GaussToStringStyle.DEFAULT_STYLE);
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Table other = (Table) obj;
        if (name == null) {
            if (other.name != null) return false;
        } else if (!name.equals(other.name)) return false;
        if (schema == null) {
            if (other.schema != null) return false;
        } else if (!schema.equals(other.schema)) return false;
        if (type == null) {
            if (other.type != null) return false;
        } else if (!type.equals(other.type)) return false;
        return true;
    }

}

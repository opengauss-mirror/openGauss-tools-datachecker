package com.gauss.applier;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.gauss.common.db.meta.ColumnMeta;
import com.gauss.common.db.meta.ColumnValue;
import com.gauss.common.db.meta.Table;
import com.gauss.common.lifecycle.AbstractGaussLifeCycle;
import com.gauss.common.model.record.Record;
import com.gauss.exception.GaussException;

public abstract class AbstractRecordApplier extends AbstractGaussLifeCycle implements RecordApplier {

    public static class TableSqlUnit {

        public String               applierSql;
        public Map<String, Integer> applierIndexs;
    }

    protected Integer getIndex(final Map<String, Integer> indexs, ColumnValue cv, boolean notExistReturnNull) {
        Integer result = indexs.get(cv.getColumn().getName());
        if (result == null && !notExistReturnNull) {
            throw new GaussException("not found column[" + cv.getColumn().getName() + "] in record");
        } else {
            return result;
        }
    }

    /**
     * 检查下是否存在必要的字段
     */
    protected void checkColumns(Table meta, Map<String, Integer> indexs) {
        Set<String> idx = new HashSet<String>();
        for (ColumnMeta column : meta.getColumns()) {
            idx.add(column.getName());
        }

        for (ColumnMeta column : meta.getPrimaryKeys()) {
            idx.add(column.getName());
        }

        for (String key : indexs.keySet()) {
            if (!idx.contains(key)) {
                throw new GaussException("not found column[" + key + "] in target db");
            }
        }
    }

    /**
     * 获取主键字段信息
     */
    protected List<ColumnMeta> getPrimaryMetas(Record record) {
        List<ColumnMeta> result = Lists.newArrayList();
        for (ColumnValue col : record.getPrimaryKeys()) {
            result.add(col.getColumn());
        }
        return result;
    }

    /**
     * 获取普通列字段信息
     */
    protected List<ColumnMeta> getColumnMetas(Record record) {
        List<ColumnMeta> result = Lists.newArrayList();
        for (ColumnValue col : record.getColumns()) {
            result.add(col.getColumn());
        }
        return result;
    }

    /**
     * 获取主键字段信息
     */
    protected String[] getPrimaryNames(Record record) {
        String[] result = new String[record.getPrimaryKeys().size()];
        int i = 0;
        for (ColumnValue col : record.getPrimaryKeys()) {
            result[i++] = col.getColumn().getName();
        }
        return result;
    }

    /**
     * 获取普通列字段信息
     */
    protected String[] getColumnNames(Record record) {
        String[] result = new String[record.getColumns().size()];
        int i = 0;
        for (ColumnValue col : record.getColumns()) {
            result[i++] = col.getColumn().getName();
        }
        return result;
    }

}

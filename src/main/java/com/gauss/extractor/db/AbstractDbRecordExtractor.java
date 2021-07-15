package com.gauss.extractor.db;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.gauss.common.db.meta.ColumnMeta;
import com.gauss.common.db.meta.ColumnValue;
import com.gauss.common.utils.GaussUtils;
import com.gauss.extractor.AbstractRecordExtractor;

public abstract class AbstractDbRecordExtractor extends AbstractRecordExtractor {

    /**
     * 从resultset中得到value
     */
    public ColumnValue getColumnValue(ResultSet rs, String encoding, ColumnMeta col) throws SQLException {
        Object value = null;
        if (GaussUtils.isCharType(col.getType())) {
            value = rs.getString(col.getName());
        } else if (GaussUtils.isClobType(col.getType())) {
            value = rs.getString(col.getName());
        } else if (GaussUtils.isBlobType(col.getType())) {
            value = rs.getBytes(col.getName());
        } else {
            value = rs.getObject(col.getName());
        }

        return new ColumnValue(col.clone(), value);
    }
}

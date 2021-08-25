package com.gauss.common.audit;

import java.text.MessageFormat;
import java.util.List;

import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gauss.common.db.meta.ColumnValue;
import com.gauss.common.model.record.Record;

/**
 * show unmatched data
 */
public class RecordDiffer {

    private static final String SEP           = SystemUtils.LINE_SEPARATOR;
    private static String       record_format = null;
    private static final Logger diffLogger    = LoggerFactory.getLogger("check");

    static {
        record_format = SEP + "-----------------" + SEP;
        record_format += "- Schema: {0} , Table: {1}" + SEP;
        record_format += "-----------------" + SEP;
        record_format += "---Columns" + SEP;
        record_format += "{2}" + SEP;
        record_format += "---diff" + SEP;
        record_format += "\t{3}" + SEP;
    }

    public static void diff(Record record) {
        diffLogger.info(diffMessage(record.getSchemaName(),
            record.getTableName(),
            record.getColumns(),
            "record not found"));
    }

    private static String diffMessage(String schemaName, String tableName, List<ColumnValue> primaryKeys, String message) {
        return MessageFormat.format(record_format,
            schemaName,
            tableName,
            RecordDumper.dumpRecordColumns(primaryKeys),
            message);
    }
}

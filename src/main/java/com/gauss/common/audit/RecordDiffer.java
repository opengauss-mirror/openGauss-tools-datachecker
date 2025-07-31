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

package com.gauss.common.audit;

import java.text.MessageFormat;
import java.util.List;

import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gauss.common.db.meta.ColumnValue;
import com.gauss.common.model.DbType;
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
        record_format += "Database: {0}" + SEP;
        record_format += "-----------------" + SEP;
        record_format += "- Schema: {1} , Table: {2}" + SEP;
        record_format += "-----------------" + SEP;
        record_format += "---Columns" + SEP;
        record_format += "{3}" + SEP;
        record_format += "---diff" + SEP;
        record_format += "\t{4}" + SEP;
    }

    public static void diff(DbType dbType, String schemaName, String tableName, List<ColumnValue> columns) {
        diffLogger.info(diffMessage(dbType, schemaName, tableName, columns,
                "record not found in opposite database"));
    }

    private static String diffMessage(DbType dbType, String schemaName, String tableName, List<ColumnValue> primaryKeys, String message) {
        return MessageFormat.format(record_format,
            dbType,
            schemaName,
            tableName,
            RecordDumper.dumpRecordColumns(primaryKeys),
            message);
    }
}

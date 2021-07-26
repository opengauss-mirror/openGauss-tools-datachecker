package com.gauss.applier;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.MigrateMap;
import com.gauss.common.db.RecordDiffer;
import com.gauss.common.db.meta.ColumnMeta;
import com.gauss.common.db.meta.ColumnValue;
import com.gauss.common.db.meta.Table;
import com.gauss.common.db.meta.TableMetaGenerator;
import com.gauss.common.db.sql.SqlTemplates;
import com.gauss.common.model.DbType;
import com.gauss.common.model.GaussContext;
import com.gauss.common.model.record.Record;
import com.gauss.common.utils.GaussUtils;
import com.gauss.exception.GaussException;

/**
 * 数据对比
 */
public class CheckRecordApplier extends AbstractRecordApplier {

    protected static final Logger             logger = LoggerFactory.getLogger(CheckRecordApplier.class);
    protected Map<List<String>, TableSqlUnit> selectSqlCache;
    protected Map<List<String>, Table>        tableCache;
    protected GaussContext                   context;
    protected DbType                          dbType;

    public CheckRecordApplier(GaussContext context){
        this.context = context;
    }

    public void start() {
        super.start();

        dbType = GaussUtils.judgeDbType(context.getTargetDs());
        tableCache = MigrateMap.makeComputingMap(new Function<List<String>, Table>() {

            public Table apply(List<String> names) {
                if (names.size() != 2) {
                    throw new GaussException("names[" + names.toString() + "] is not valid");
                }

                return TableMetaGenerator.getTableMeta(context.getTargetDs(),
                    names.get(0),
                    names.get(1));
            }
        });

        selectSqlCache = MigrateMap.makeMap();
    }

    public void stop() {
        super.stop();
    }

    public void apply(List<Record> records) throws GaussException {
        // no one,just return
        if (GaussUtils.isEmpty(records)) {
            return;
        }

        doApply(records);
    }

    protected void doApply(List<Record> records) {
        Map<List<String>, List<Record>> buckets = MigrateMap.makeComputingMap(new Function<List<String>, List<Record>>() {

            public List<Record> apply(List<String> names) {
                return Lists.newArrayList();
            }
        });

        // 根据目标库的不同，划分为多个bucket
        for (Record record : records) {
            buckets.get(Arrays.asList(record.getSchemaName(), record.getTableName())).add(record);
        }

        JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getTargetDs());
        for (final List<Record> batchRecords : buckets.values()) {
            List<Record> queryRecords = null;
            queryRecords = queryOneByOne(jdbcTemplate, batchRecords);
            diff(batchRecords, queryRecords);
        }
    }

    /**
     * 一条条记录串行处理
     */
    protected List<Record> queryOneByOne(JdbcTemplate jdbcTemplate, final List<Record> records) {
        TableSqlUnit sqlUnit = getSqlUnit(records.get(0));
        String selectSql = sqlUnit.applierSql;
        final Map<String, Integer> indexs = sqlUnit.applierIndexs;
        final List<ColumnMeta> primaryKeys = getPrimaryMetas(records.get(0));
        final List<ColumnMeta> columns = getColumnMetas(records.get(0));
        return (List<Record>) jdbcTemplate.execute(selectSql, new PreparedStatementCallback() {

            public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                List<Record> result = Lists.newArrayList();
                for (int i = 0;i < records.size();i++) {
                    for (ColumnValue col : records.get(i).getColumns()) {
                        Integer index = getIndex(indexs, col, true);
                        if (index != null) {
                            ps.setObject(index, col.getValue());
                        }
                    }
                    ResultSet rs = ps.executeQuery();
                    if (!rs.next()) {
                        result.add(null);
                    } else {
                        List<ColumnValue> cms = new ArrayList<ColumnValue>();
                        List<ColumnValue> pks = new ArrayList<ColumnValue>();
                        for (ColumnMeta pk : primaryKeys) {
                            ColumnValue cv = getColumnValue(rs, getTargetEncoding(), pk);
                            pks.add(cv);
                        }

                        for (ColumnMeta col : columns) {
                            ColumnValue cv = getColumnValue(rs, getTargetEncoding(), col);
                            cms.add(cv);
                        }

                        Record re = new Record(records.get(i).getSchemaName(), records.get(i).getTableName(), pks, cms);
                        result.add(re);
                    }
                }
                return result;
            }
        });
    }

    protected String getTargetEncoding() {
        if (dbType.isOracle()) {
            return context.getTargetEncoding();
        } else {
            return null;
        }
    }

    protected ColumnValue getColumnValue(ResultSet rs, String encoding, ColumnMeta col) throws SQLException {
        Object value = null;
        if (col.getType() == Types.DATE) {
            value = rs.getTimestamp(col.getName());
            col = new ColumnMeta(col.getName(), Types.TIMESTAMP);
        } else if (col.getType() == Types.TIMESTAMP) {
            value = rs.getTimestamp(col.getName());
            col = new ColumnMeta(col.getName(), Types.TIMESTAMP);
        } else if (GaussUtils.isCharType(col.getType())) {
            value = rs.getString(col.getName());
        } else if (GaussUtils.isClobType(col.getType())) {
            value = rs.getString(col.getName());
        } else if (GaussUtils.isBlobType(col.getType())) {
            value = rs.getBytes(col.getName());
        } else {
            value = rs.getObject(col.getName());
        }

        return new ColumnValue(col, value);
    }

    /**
     * @param records1 源库的数据
     * @param records2 目标库的数据
     */
    protected void diff(List<Record> records1, List<Record> records2) {
        for (int i = 0;i < records1.size();i++) {
            RecordDiffer.diff(records1.get(i), records2.get(i));
        }
    }

    protected TableSqlUnit getSqlUnit(Record record) {
        List<String> names = Arrays.asList(record.getSchemaName(), record.getTableName());
        String tableName = record.getSchemaName()+"."+record.getTableName();
        tableName = tableName.intern();
        TableSqlUnit sqlUnit = selectSqlCache.get(names);
        if (sqlUnit == null) {
            synchronized (tableName) {
                sqlUnit = selectSqlCache.get(names);
                if (sqlUnit == null) { // double-check
                    sqlUnit = new TableSqlUnit();
                    String applierSql = null;
                    Table meta = TableMetaGenerator.getTableMeta(context.getTargetDs(),
                        names.get(0),
                        names.get(1));

                    String[] primaryKeys = getPrimaryNames(record);
                    String[] columns = getColumnNames(record);
                    applierSql = SqlTemplates.COMMON.getSelectSql(meta.getSchema(),
                        meta.getName(),
                        primaryKeys,
                        columns);

                    int index = 1;
                    Map<String, Integer> indexs = new HashMap<String, Integer>();
                    for (String column : primaryKeys) {
                        indexs.put(column, index);
                        index++;
                    }

                    if (index == 1) { // 没有主键
                        for (String column : columns) {
                            indexs.put(column, index);
                            index++;
                        }
                    }

                    // 检查下是否少了列
                    checkColumns(meta, indexs);

                    sqlUnit.applierSql = applierSql;
                    sqlUnit.applierIndexs = indexs;
                    selectSqlCache.put(names, sqlUnit);
                }
            }
        }

        return sqlUnit;
    }
}

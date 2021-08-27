package com.gauss.comparer;

import com.gauss.common.audit.RecordDiffer;
import com.gauss.common.db.meta.ColumnMeta;
import com.gauss.common.db.meta.ColumnValue;
import com.gauss.common.db.meta.Table;
import com.gauss.common.db.sql.SqlTemplate;
import com.gauss.common.model.DbType;
import com.gauss.common.model.GaussContext;
import com.gauss.common.model.record.Record;
import com.gauss.common.utils.GaussUtils;
import com.gauss.exception.GaussException;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GaussRecordComparer extends AbstractRecordComparer {
    String compareSql;

    DbType dbType;

    GaussContext context;

    int query_dop;

    private String orinTableName;

    private String compareTableName;

    public GaussRecordComparer(DbType dbType, GaussContext context, int query_dop) {
        this.query_dop = query_dop;
        this.dbType = dbType;
        this.context = context;
        Table tableMeta = context.getTableMeta();
        this.orinTableName = tableMeta.getFullName();
        this.compareTableName = tableMeta.getSchema() + ".A" + tableMeta.getName() + "_dataChecker";
    }

    @Override
    public void start() {
        super.start();
    }
    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void compare() throws GaussException {
        SqlTemplate sqlTemplate = new SqlTemplate(dbType, context);
        compareSql = sqlTemplate.getCompareSql();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getTargetDs());
        jdbcTemplate.execute("set query_dop to " + query_dop + ";");
        SqlRowSet result = jdbcTemplate.queryForRowSet(compareSql);
        ArrayList<String> diffSource = new ArrayList<>();
        ArrayList<String> diffTarget = new ArrayList<>();
        while (result.next()) {
            if (result.getString(1) != null) {
                diffSource.add(result.getString(1));
            } else {
                diffTarget.add(result.getString(2));
            }
        }
        if (!diffSource.isEmpty()) {
            GaussUtils.outputUnnormal("Source table : " + orinTableName);
            searchFromDb(sqlTemplate.getSearchSql(diffSource),dbType);
        }

        if (!diffTarget.isEmpty()) {
            GaussUtils.outputUnnormal("Target table : " + orinTableName);
            searchFromDb(new SqlTemplate(DbType.OPGS, context).getSearchSql(diffTarget),DbType.OPGS);
        }

        jdbcTemplate.execute("drop table " + compareTableName + "A;");
        jdbcTemplate.execute("drop table " + compareTableName + "B;");
    }

    public void searchFromDb(String searchSql, DbType dbType) {
        SqlRowSet rs;
        if (dbType == DbType.OPGS) {
            rs = new JdbcTemplate(context.getTargetDs()).queryForRowSet(searchSql);
        } else {
            rs = new JdbcTemplate(context.getSourceDs()).queryForRowSet(searchSql);
        }

        while (rs.next()) {
            List<ColumnValue> cms = new ArrayList<ColumnValue>();

            for (ColumnMeta col : context.getTableMeta().getColumns()) {
                ColumnValue cv = null;
                try {
                    cv = getColumnValue(rs, col);
                } catch (SQLException throwable) {
                    throwable.printStackTrace();
                }
                cms.add(cv);
            }

            Record re = new Record(context.getTableMeta().getSchema(), context.getTableMeta().getName(), cms);
            RecordDiffer.diff(re);
        }
    }

    /**
     * get value from SqlRowSet
     */
    public ColumnValue getColumnValue(SqlRowSet rs, ColumnMeta col) throws SQLException {
        Object value = null;
        if (GaussUtils.isCharType(col.getType())) {
            value = rs.getString(col.getName());
        } else if (GaussUtils.isClobType(col.getType())) {
            value = rs.getString(col.getName());
        } else {
            value = rs.getObject(col.getName());
        }
        return new ColumnValue(col.clone(), value);
    }
}
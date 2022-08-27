package com.gauss.comparer;

import com.gauss.common.audit.RecordDiffer;
import com.gauss.common.db.meta.ColumnMeta;
import com.gauss.common.db.meta.ColumnValue;
import com.gauss.common.db.meta.Table;
import com.gauss.common.db.sql.SqlFactory;
import com.gauss.common.db.sql.SqlTemplate;
import com.gauss.common.model.DbType;
import com.gauss.common.model.GaussContext;
import com.gauss.common.utils.GaussUtils;
import com.gauss.common.utils.Quote;
import com.gauss.exception.GaussException;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class GaussRecordComparer extends AbstractRecordComparer {
    DbType dbType;

    GaussContext context;

    int query_dop;

    private String orinTableName;

    private String srcCompareTableName;

    private String destCompareTableName;

    static final int maxInValues = 9999;

    public GaussRecordComparer(DbType dbType, GaussContext context, int query_dop) {
        this.query_dop = query_dop;
        this.dbType = dbType;
        this.context = context;
        Table tableMeta = context.getTableMeta();
        this.orinTableName = "\"" + tableMeta.getSchema() + "\".\"" + tableMeta.getName() + "\"";
        this.srcCompareTableName = Quote.join("", Quote.ins.quote(tableMeta.getSchema()), ".",
            Quote.ins.quote(tableMeta.getName() + "_dataCheckerA"));
        this.destCompareTableName = Quote.join("", Quote.ins.quote(tableMeta.getSchema()), ".",
            Quote.ins.quote(tableMeta.getName() + "_dataCheckerB"));
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
        SqlFactory sqlFactory = new SqlFactory();
        SqlTemplate sqlTemplate = sqlFactory.getSqlTemplate(DbType.OPGS, dbType, context);
        SqlTemplate sqlTemplate2 = sqlFactory.getSqlTemplate(dbType, dbType, context);
        String compareSql = sqlTemplate.getCompareSql();
        JdbcTemplate jdbcTemplateOpgs = new JdbcTemplate(context.getTargetDs());
        JdbcTemplate jdbcTemplateSource = new JdbcTemplate(context.getSourceDs());
        jdbcTemplateOpgs.execute("set query_dop to " + query_dop + ";");
        jdbcTemplateOpgs.execute("set session_timeout to 0;");
        SqlRowSet result = jdbcTemplateOpgs.queryForRowSet(compareSql);
        boolean isSrcDiff = false;
        boolean isTargetDiff = false;
        ArrayList<String> diffSource = new ArrayList<>();
        ArrayList<String> diffTarget = new ArrayList<>();
        while (result.next()) {
            if (result.getString(1) != null) {
                diffSource.add(result.getString(1));
            } else {
                diffTarget.add(result.getString(2));
            }

            if (diffSource.size() >= maxInValues) {
                isSrcDiff = true;
                searchFromDb(sqlTemplate2.getSearchSql(diffSource), dbType, jdbcTemplateSource);
                diffSource.clear();
            }

            if (diffTarget.size() >= maxInValues) {
                isTargetDiff = true;
                searchFromDb(sqlTemplate.getSearchSql(diffTarget), DbType.OPGS, jdbcTemplateOpgs);
                diffTarget.clear();
            }
        }

        if (!diffSource.isEmpty()) {
            isSrcDiff = true;
            searchFromDb(sqlTemplate2.getSearchSql(diffSource), dbType, jdbcTemplateSource);
            diffSource.clear();
        }

        if (!diffTarget.isEmpty()) {
            isTargetDiff = true;
            searchFromDb(sqlTemplate.getSearchSql(diffTarget), DbType.OPGS, jdbcTemplateOpgs);
            diffTarget.clear();
        }

        if (isSrcDiff) {
            GaussUtils.outputUnnormal("Source table : " + orinTableName);
        }

        if (isTargetDiff) {
            GaussUtils.outputUnnormal("Target table : " + orinTableName);
        }

        jdbcTemplateOpgs.execute("drop table " + srcCompareTableName);
        jdbcTemplateOpgs.execute("drop table " + destCompareTableName);
    }

    public void searchFromDb(String searchSql, DbType dbType, JdbcTemplate jdbcTemplate) {
        SqlRowSet rs = jdbcTemplate.queryForRowSet(searchSql);
        Table meta = context.getTableMeta();
        List<ColumnValue> cms = new ArrayList<ColumnValue>();

        while (rs.next()) {
            for (ColumnMeta col : meta.getColumns()) {
                ColumnValue cv = null;
                try {
                    cv = getColumnValue(rs, col);
                } catch (SQLException throwable) {
                    throwable.printStackTrace();
                }
                cms.add(cv);
            }

            RecordDiffer.diff(dbType, meta.getSchema(), meta.getName(), cms);
            cms.clear();
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
        } else if (col.getType() == Types.TIMESTAMP) {
            value = rs.getObject(col.getName());
            if (value instanceof Timestamp) {
                value = ((Timestamp) value).toLocalDateTime();
            }
        } else {
            value = rs.getObject(col.getName());
        }
        return new ColumnValue(col.clone(), value);
    }
}
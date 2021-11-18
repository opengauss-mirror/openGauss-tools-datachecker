package com.gauss.preparer;

import com.gauss.common.db.meta.Table;
import com.gauss.common.db.sql.SqlFactory;
import com.gauss.common.db.sql.SqlTemplate;
import com.gauss.common.model.DbType;
import com.gauss.common.model.GaussContext;
import com.gauss.common.model.PrepareStatus;
import com.gauss.common.utils.Quote;
import com.gauss.common.utils.thread.NamedThreadFactory;
import com.gauss.exception.GaussException;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.StatementCallback;

import java.sql.SQLException;
import java.sql.Statement;

public class GaussRecordPreparer extends AbstractRecordPreparer {
    String prepareSql;

    int query_dop;

    GaussContext context;

    public Thread prepareThread = null;

    String srcCompareTableName;

    String destCompareTableName;

    JdbcTemplate jdbcTemplate;

    DbType srcType;

    private volatile boolean success = true;

    public boolean isSuccess() {
        return success;
    }

    public GaussRecordPreparer(DbType srcType, GaussContext context, int query_dop) {
        this.context = context;
        this.query_dop = query_dop;
        this.srcType = srcType;
        Table tableMeta = context.getTableMeta();
        this.srcCompareTableName = Quote.join("", Quote.ins.quote(tableMeta.getSchema()),
                ".", Quote.ins.quote(tableMeta.getName() + "_dataCheckerA"));
        this.destCompareTableName = Quote.join("", Quote.ins.quote(tableMeta.getSchema()),
                ".", Quote.ins.quote(tableMeta.getName() + "_dataCheckerB"));
    }

    @Override
    public void start() {
        super.start();
        jdbcTemplate = new JdbcTemplate(context.getTargetDs());
        dropTable();
        createTable();
        if (StringUtils.isEmpty(prepareSql)) {
            SqlFactory sqlFactory = new SqlFactory();
            SqlTemplate sqlTemplate = sqlFactory.getSqlTemplate(DbType.OPGS, srcType, context);
            prepareSql = sqlTemplate.getPrepareSql();
        }

        // Asynchronous thread
        prepareThread = new NamedThreadFactory(
            this.getClass().getSimpleName() + "-" + context.getTableMeta().getFullName()).newThread(
            new Preparer(context));
        prepareThread.start();
    }

    @Override
    public void stop() {
        super.stop();
        prepareThread.interrupt();
        try {
            prepareThread.join(2 * 1000);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public class Preparer implements Runnable {

        private JdbcTemplate jdbcTemplate;

        public Preparer(GaussContext context) {
            jdbcTemplate = new JdbcTemplate(context.getTargetDs());
        }

        public void run() {
            jdbcTemplate.execute("set query_dop to " + query_dop + ";");
            jdbcTemplate.execute("set session_timeout to 0;");
            if (srcType == DbType.MYSQL) {
                jdbcTemplate.execute("set behavior_compat_options to 'display_leading_zero';");
            } else {
                jdbcTemplate.execute("set behavior_compat_options to '';");
            }
            jdbcTemplate.execute(new StatementCallback() {
                public Object doInStatement(Statement stmt) {
                    try {
                        stmt.execute(prepareSql);
                    } catch (SQLException e) {
                        success = false;
                        throw new GaussException(e);
                    }
                    return null;
                }
            });
            setStatus(PrepareStatus.END);
        }
    }

    public void createTable() {
        jdbcTemplate.execute("create unlogged table " + srcCompareTableName + "(checksumA text);");
        jdbcTemplate.execute("create unlogged table " + destCompareTableName + "(checksumB text);");
    }
    public void dropTable() {
        jdbcTemplate.execute("drop table if exists " + srcCompareTableName);
        jdbcTemplate.execute("drop table if exists " + destCompareTableName);
    }
}

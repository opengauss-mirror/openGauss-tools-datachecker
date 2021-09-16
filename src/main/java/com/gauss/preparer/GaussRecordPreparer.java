package com.gauss.preparer;

import com.gauss.common.db.meta.Table;
import com.gauss.common.db.sql.OpenGaussUtil;
import com.gauss.common.model.GaussContext;
import com.gauss.common.model.PrepareStatus;
import com.gauss.common.utils.thread.NamedThreadFactory;
import com.gauss.exception.GaussException;

import org.apache.commons.lang.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.StatementCallback;

import java.sql.SQLException;
import java.sql.Statement;

public class GaussRecordPreparer extends AbstractRecordPreparer {
    String prepareSql;

    int query_dop;

    GaussContext context;

    public Thread prepareThread = null;

    String compareTableName;

    JdbcTemplate jdbcTemplate;

    private volatile boolean success = true;

    public boolean isSuccess() {
        return success;
    }

    public GaussRecordPreparer(GaussContext context, int query_dop) {
        this.context = context;
        this.query_dop = query_dop;
        Table tableMeta = context.getTableMeta();
        compareTableName = "\"" + tableMeta.getSchema() + "\".\"" + tableMeta.getName() + "_dataChecker";
    }

    @Override
    public void start() {
        super.start();
        jdbcTemplate = new JdbcTemplate(context.getTargetDs());
        dropTable();
        createTable();
        if (StringUtils.isEmpty(prepareSql)) {
            OpenGaussUtil openGaussUtil = new OpenGaussUtil(context);
            prepareSql = openGaussUtil.getPrepareSql();
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
        jdbcTemplate.execute("create unlogged table " + compareTableName + "A\"(checksumA text);");
        jdbcTemplate.execute("create unlogged table " + compareTableName + "B\"(checksumB text);");
    }
    public void dropTable() {
        jdbcTemplate.execute("drop table if exists " + compareTableName + "A\";");
        jdbcTemplate.execute("drop table if exists " + compareTableName + "B\";");
    }
}

package com.gauss.preparer;

import com.gauss.common.db.meta.Table;
import com.gauss.common.db.sql.SqlTemplate;
import com.gauss.common.model.DbType;
import com.gauss.common.model.GaussContext;
import com.gauss.common.model.PrepareStatus;
import com.gauss.common.utils.GaussUtils;
import com.gauss.common.utils.thread.NamedThreadFactory;

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

    private Thread prepareThread = null;

    String compareTableName;

    public GaussRecordPreparer(GaussContext context, int query_dop) {
        this.context = context;
        this.query_dop = query_dop;
        Table tableMeta = context.getTableMeta();
        compareTableName = tableMeta.getSchema() + ".A" + tableMeta.getName() + "_dataChecker";
    }

    @Override
    public void start() {
        super.start();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getTargetDs());
        jdbcTemplate.execute("drop table if exists " + compareTableName + "A;");
        jdbcTemplate.execute("create unlogged table " + compareTableName + "A(checksumA text);");
        jdbcTemplate.execute("drop table if exists " + compareTableName + "B;");
        jdbcTemplate.execute("create unlogged table " + compareTableName + "B(checksumB text);");
        if (StringUtils.isEmpty(prepareSql)) {
            SqlTemplate sqlTemplate = new SqlTemplate(DbType.OPGS, context);
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
    }

    public class Preparer implements Runnable {

        private JdbcTemplate jdbcTemplate;

        public Preparer(GaussContext context) {
            jdbcTemplate = new JdbcTemplate(context.getTargetDs());
        }

        public void run() {
            jdbcTemplate.execute("set query_dop to " + query_dop + ";");
            jdbcTemplate.execute(new StatementCallback() {
                public Object doInStatement(Statement stmt) throws SQLException, DataAccessException {
                    stmt.execute(prepareSql);
                    return null;
                }
            });
            setStatus(PrepareStatus.END);
        }
    }
}

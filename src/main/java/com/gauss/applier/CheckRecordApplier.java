package com.gauss.applier;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.gauss.common.model.DbType;
import com.gauss.common.utils.Quote;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.opengauss.copy.CopyManager;
import org.opengauss.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import com.gauss.common.db.meta.Table;
import com.gauss.common.lifecycle.AbstractGaussLifeCycle;
import com.gauss.common.model.GaussContext;
import com.gauss.common.utils.GaussUtils;
import com.gauss.exception.GaussException;

/**
 * copy checksum into target database
 */
public class CheckRecordApplier extends AbstractGaussLifeCycle implements RecordApplier {

    protected static final Logger logger = LoggerFactory.getLogger(CheckRecordApplier.class);

    protected GaussContext context;
    private int query_dop;
    String compareTableName;

    private static final String SEPARATOR = System.lineSeparator();

    public CheckRecordApplier(GaussContext context, int query_dop) {
        this.context = context;
        this.query_dop = query_dop;
        Table tableMeta = context.getTableMeta();
        this.compareTableName =  Quote.join("",
                Quote.ins.quote(tableMeta.getSchema()), ".",
                Quote.ins.quote(tableMeta.getName() + "_dataCheckerA"));
    }

    @Override
    public void start() {
        super.start();
    }

    public void apply(List<String> records) throws GaussException {
        // no one,just return
        if (GaussUtils.isEmpty(records)) {
            return;
        }
        try {
            doApply(records);
        } catch (IOException | SQLException e) {
            logger.error("## Something goes wrong when inserting checksum:\n{}", ExceptionUtils.getFullStackTrace(e));
            System.exit(0);
        }
    }

    protected void doApply(List<String> records) throws IOException, SQLException {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getTargetDs());
        jdbcTemplate.execute("set query_dop to " + query_dop + ";");
        jdbcTemplate.execute("set session_timeout to 0;");
        jdbcTemplate.execute(new ConnectionCallback() {
            @Override
            public Object doInConnection(Connection connection) throws SQLException, DataAccessException {
                StringBuilder buffer = new StringBuilder();
                records.stream().forEach((String record) -> {buffer.append(record).append(SEPARATOR);});

                String sql = "copy " + compareTableName + " from stdin";
                BaseConnection baseConn = (BaseConnection) (connection.getMetaData().getConnection());
                CopyManager cp = new CopyManager(baseConn);
                StringReader reader = new StringReader(buffer.toString());
                try {
                    cp.copyIn(sql, reader);
                } catch (IOException e) {
                    logger.error("## Something goes wrong when inserting checksum:\n{}",
                        ExceptionUtils.getFullStackTrace(e));
                    System.exit(0);
                }
                return null;
            }
        });
    }
}

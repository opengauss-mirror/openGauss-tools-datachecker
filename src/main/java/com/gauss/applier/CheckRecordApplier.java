package com.gauss.applier;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

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

    private static final String SEPARATOR = System.lineSeparator();

    public CheckRecordApplier(GaussContext context, int query_dop) {
        this.context = context;
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
        jdbcTemplate.execute(new ConnectionCallback() {
            @Override
            public Object doInConnection(Connection connection) throws SQLException, DataAccessException {
                StringBuilder buffer = new StringBuilder();
                records.stream().forEach((String record) -> {buffer.append(record).append(SEPARATOR);});

                String sql = "copy " + context.getTableMeta().getFullName() + "_dataCheckerA " + "from stdin";
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

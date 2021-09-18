package com.gauss.extractor.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.gauss.common.db.sql.*;
import org.apache.commons.lang.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.StatementCallback;

import com.gauss.common.model.DbType;
import com.gauss.extractor.AbstractRecordExtractor;
import com.google.common.collect.Lists;
import com.gauss.common.model.ExtractStatus;
import com.gauss.common.model.ProgressStatus;
import com.gauss.common.model.GaussContext;
import com.gauss.common.utils.thread.NamedThreadFactory;
import com.gauss.exception.GaussException;

/**
 * extract checksum from source table
 */
public class DbOnceFullRecordExtractor extends AbstractRecordExtractor {

    private String extractSql;

    private LinkedBlockingQueue<String> queue;

    private Thread extractorThread = null;

    private GaussContext context;

    private DbType dbType;

    public DbOnceFullRecordExtractor(GaussContext context, DbType dbType) {
        this.context = context;
        this.dbType = dbType;
    }
    @Override
    public void start() {
        super.start();
        if (StringUtils.isEmpty(extractSql)) {
            SqlFactory sqlFactory = new SqlFactory();
            SqlTemplate sqlTemplate = sqlFactory.getSqlTemplate(dbType, context);
            extractSql = sqlTemplate.getExtractSql();
        }

        // 启动异步线程
        if (dbType == DbType.ORACLE) {
            //Oracle
            extractorThread = new NamedThreadFactory(
                this.getClass().getSimpleName() + "-" + context.getTableMeta().getFullName()).newThread(
                new OracleContinueExtractor(context));
        } else {
            //Mysql
            extractorThread = new NamedThreadFactory(
                this.getClass().getSimpleName() + "-" + context.getTableMeta().getFullName()).newThread(
                new MysqlContinueExtractor(context));
        }

        extractorThread.start();

        queue = new LinkedBlockingQueue<String>(context.getOnceCrawNum() * 2);
        tracer.update(context.getTableMeta().getFullName(), ProgressStatus.FULLING);
    }
    @Override
    public void stop() {
        super.stop();
        extractorThread.interrupt();
        
        try {
            extractorThread.join(2 * 1000);
        } catch (InterruptedException e) {
            // ignore
        }
        tracer.update(context.getTableMeta().getFullName(), ProgressStatus.SUCCESS);
    }

    public List<String> extract() throws GaussException {
        List<String> records = Lists.newArrayListWithCapacity(context.getOnceCrawNum());
        for (int i = 0; i < context.getOnceCrawNum(); i++) {
            String r = queue.poll();
            if (r != null) {
                records.add(r);
            } else if (getStatus() == ExtractStatus.TABLE_END) {
                // 验证下是否已经结束了
                String r1 = queue.poll();
                if (r1 != null) {
                    records.add(r1);
                } else {
                    // 已经取到低了，没有数据了
                    break;
                }
            } else {
                // 没去到数据
                i--;
            }
        }

        return records;
    }

    public class MysqlContinueExtractor implements Runnable {

        private JdbcTemplate jdbcTemplate;

        public MysqlContinueExtractor(GaussContext context) {
            jdbcTemplate = new JdbcTemplate(context.getSourceDs());
        }

        public void run() {
            jdbcTemplate.execute("SET NAMES utf8;");
            jdbcTemplate.execute(new ConnectionCallback() {
                public Object doInConnection(Connection conn) {
                    try {
                        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                        stmt.setFetchSize(Integer.MIN_VALUE);
                        stmt.setFetchDirection(ResultSet.FETCH_REVERSE);
                        stmt.execute(extractSql);
                        ResultSet rs = stmt.getResultSet();
                        while (rs.next()) {
                            queue.put(rs.getString(1));
                        }
                        rs.close();
                        stmt.close();
                    } catch (SQLException | InterruptedException e) {
                        throw new GaussException(e);
                    } finally {
                        setStatus(ExtractStatus.TABLE_END);
                    }
                    return null;
                }
            });
        }
    }

    public class OracleContinueExtractor implements Runnable {

        private JdbcTemplate jdbcTemplate;

        public OracleContinueExtractor(GaussContext context) {
            jdbcTemplate = new JdbcTemplate(context.getSourceDs());
        }

        public void run() {
            jdbcTemplate.execute(new StatementCallback() {

                public Object doInStatement(Statement stmt) throws SQLException {
                    try {
                        stmt.setFetchSize(0);
                        stmt.execute(extractSql);
                        ResultSet rs = stmt.getResultSet();
                        while (rs.next()) {
                            queue.put(rs.getString(1));
                        }
                        rs.close();
                    } catch (SQLException | InterruptedException e) {
                        throw new GaussException(e);
                    } finally {
                        setStatus(ExtractStatus.TABLE_END);
                    }
                    return null;
                }
            });

        }
    }
}

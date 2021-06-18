package com.gauss.extractor.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.collect.Lists;
import com.gauss.common.db.meta.ColumnMeta;
import com.gauss.common.db.meta.ColumnValue;
import com.gauss.common.db.sql.SqlTemplates;
import com.gauss.common.model.ExtractStatus;
import com.gauss.common.model.ProgressStatus;
import com.gauss.common.model.GaussContext;
import com.gauss.common.model.record.Record;
import com.gauss.common.utils.thread.NamedThreadFactory;
import com.gauss.exception.GaussException;

/**
 * 基于mysql的一次性任务
 */
public class MysqlOnceFullRecordExtractor extends AbstractMysqlRecordExtractor {

    private static final String         FORMAT          = "select /*+parallel(t)*/ {0} from {1}.{2} t";
    private StringBuffer                      extractSql;
    private LinkedBlockingQueue<Record> queue;
    private Thread                      extractorThread = null;
    private GaussContext               context;

    public MysqlOnceFullRecordExtractor(GaussContext context){
        this.context = context;
    }

    public void start() {
        super.start();

        if (StringUtils.isEmpty(extractSql.toString())) {
            String columns = SqlTemplates.COMMON.makeColumn(context.getTableMeta().getColumnsWithPrimary());
            extractSql = new StringBuffer(new MessageFormat(FORMAT).format(new Object[] { columns, context.getTableMeta().getSchema(),
                    context.getTableMeta().getName() }));
        }

        // 启动异步线程
        extractorThread = new NamedThreadFactory(this.getClass().getSimpleName() + "-"
                                                 + context.getTableMeta().getFullName()).newThread(new ContinueExtractor(context));
        extractorThread.start();

        queue = new LinkedBlockingQueue<Record>(context.getOnceCrawNum() * 2);
        tracer.update(context.getTableMeta().getFullName(), ProgressStatus.FULLING);
    }

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

    public List<Record> extract() throws GaussException {
        List<Record> records = Lists.newArrayListWithCapacity(context.getOnceCrawNum());
        for (int i = 0; i < context.getOnceCrawNum(); i++) {
            Record r = queue.poll();
            if (r != null) {
                records.add(r);
            } else if (status() == ExtractStatus.TABLE_END) {
                // 验证下是否已经结束了
                Record r1 = queue.poll();
                if (r1 != null) {
                    records.add(r1);
                } else {
                    // 已经取到低了，没有数据了
                    break;
                }
            } else {
                // 没去到数据
                i--;
                continue;
            }
        }

        return records;
    }

    public class ContinueExtractor implements Runnable {

        private JdbcTemplate jdbcTemplate;

        public ContinueExtractor(GaussContext context){
            jdbcTemplate = new JdbcTemplate(context.getSourceDs());
        }

        public void run() {
            jdbcTemplate.execute(new ConnectionCallback() {

                public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
                    stmt.setFetchSize(Integer.MIN_VALUE);
                    stmt.setFetchDirection(ResultSet.FETCH_REVERSE);
                    stmt.execute(extractSql.toString());
                    ResultSet rs = stmt.getResultSet();
                    while (rs.next()) {
                        List<ColumnValue> cms = new ArrayList<ColumnValue>();
                        List<ColumnValue> pks = new ArrayList<ColumnValue>();

                        for (ColumnMeta pk : context.getTableMeta().getPrimaryKeys()) {
                            ColumnValue cv = getColumnValue(rs, context.getSourceEncoding(), pk);
                            pks.add(cv);
                        }

                        for (ColumnMeta col : context.getTableMeta().getColumns()) {
                            ColumnValue cv = getColumnValue(rs, context.getSourceEncoding(), col);
                            cms.add(cv);
                        }

                        Record re = new Record(context.getTableMeta().getSchema(),
                            context.getTableMeta().getName(),
                            pks,
                            cms);
                        try {
                            queue.put(re);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt(); // 传递
                            throw new GaussException(e);
                        }
                    }

                    setStatus(ExtractStatus.TABLE_END);
                    rs.close();
                    stmt.close();
                    return null;
                }
            });

        }
    }

    public void setExtractSql(String extractSql) {
        if (extractSql != null) {
            this.extractSql = new StringBuffer(extractSql);
        } else {
            this.extractSql = new StringBuffer();
        }
    }

}

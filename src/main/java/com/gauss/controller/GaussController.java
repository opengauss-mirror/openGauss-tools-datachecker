/*
This program is free software;
you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program;
if not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
*/

package com.gauss.controller;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.gauss.common.utils.Quote;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.MDC;
import org.springframework.jdbc.core.JdbcTemplate;

import com.gauss.comparer.GaussRecordComparer;
import com.gauss.extractor.db.DbOnceFullRecordExtractor;
import com.gauss.preparer.GaussRecordPreparer;
import com.google.common.collect.Lists;
import com.gauss.applier.CheckRecordApplier;
import com.gauss.applier.MultiThreadCheckRecordApplier;
import com.gauss.applier.RecordApplier;
import com.gauss.common.GaussConstants;
import com.gauss.common.db.DataSourceFactory;
import com.gauss.common.db.meta.Table;
import com.gauss.common.db.meta.TableMetaGenerator;
import com.gauss.common.lifecycle.AbstractGaussLifeCycle;
import com.gauss.common.model.DataSourceConfig;
import com.gauss.common.model.DbType;
import com.gauss.common.model.GaussContext;
import com.gauss.common.stats.ProgressTracer;
import com.gauss.common.stats.StatAggregation;
import com.gauss.common.utils.LikeUtil;
import com.gauss.common.utils.thread.NamedThreadFactory;
import com.gauss.exception.GaussException;
import com.gauss.extractor.RecordExtractor;

/**
 * the controller of the whole process
 */
public class GaussController extends AbstractGaussLifeCycle {

    private DataSourceFactory dataSourceFactory = new DataSourceFactory();

    private Configuration config;

    private GaussContext globalContext;

    private DbType sourceDbType = DbType.MYSQL;

    private DbType targetDbType = DbType.OPGS;

    int query_dop;

    private TableController tableController;

    private ProgressTracer progressTracer;

    private List<GaussInstance> instances = Lists.newArrayList();

    private ScheduledExecutorService schedule;

    // global thread pool
    private ThreadPoolExecutor extractorExecutor = null;

    private ThreadPoolExecutor applierExecutor = null;

    public GaussController(Configuration config) {
        this.config = config;
    }

    @Override
    public void start() {
        MDC.remove(GaussConstants.MDC_TABLE_SHIT_KEY);
        super.start();
        if (!dataSourceFactory.isStart()) {
            dataSourceFactory.start();
        }

        this.sourceDbType = DbType.valueOf(StringUtils.upperCase(config.getString("gauss.database.source.type")));
        this.targetDbType = DbType.valueOf(StringUtils.upperCase(config.getString("gauss.database.target.type")));
        this.globalContext = initGlobalContext();

        prepareBeforeStart();
        Quote.ins.setSourceType(sourceDbType);

        int statBufferSize = config.getInt("gauss.stat.buffer.size", 16384);
        int statPrintInterval = config.getInt("gauss.stat.print.interval", 5);
        query_dop = config.getInt("gauss.table.query_dop", 8);
        if (query_dop <= 0 || query_dop >= 65) {
            query_dop = 8;
        }
        // enable concurrent
        boolean concurrent = config.getBoolean("gauss.table.concurrent.enable", false);

        Collection<TableHolder> tableMetas = initTables();
        int threadSize = 1; // 默认1，代表串行
        if (concurrent) {
            threadSize = config.getInt("gauss.table.concurrent.size", 5); // 并行执行的table数
            if (threadSize <= 0) {
                threadSize = 5;
            }
        }

        tableController = new TableController(tableMetas.size(), threadSize);
        progressTracer = new ProgressTracer(tableMetas.size());

        boolean useExtractorExecutor = config.getBoolean("gauss.extractor.concurrent.global", false);
        boolean useApplierExecutor = config.getBoolean("gauss.applier.concurrent.global", false);
        if (useExtractorExecutor) {
            int extractorSize = config.getInt("gauss.extractor.concurrent.size", 300);
            extractorExecutor = new ThreadPoolExecutor(extractorSize, extractorSize, 60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(extractorSize * 2), new NamedThreadFactory("Global-Extractor"),
                new ThreadPoolExecutor.CallerRunsPolicy());
        }

        if (useApplierExecutor) {
            int applierSize = config.getInt("gauss.applier.concurrent.size", 500);
            applierExecutor = new ThreadPoolExecutor(applierSize, applierSize, 60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(applierSize * 2), new NamedThreadFactory("Global-Applier"),
                new ThreadPoolExecutor.CallerRunsPolicy());
        }
        for (TableHolder tableHolder : tableMetas) {
            GaussContext context = buildContext(globalContext, tableHolder.table);
            RecordExtractor extractor = chooseExtractor(context);
            RecordApplier applier = chooseApplier(context);
            GaussInstance instance = new GaussInstance(context);
            StatAggregation statAggregation = new StatAggregation(statBufferSize, statPrintInterval);
            instance.setPreparer(new GaussRecordPreparer(sourceDbType, context,query_dop));
            instance.setExtractor(extractor);
            instance.setApplier(applier);
            instance.setComparer(new GaussRecordComparer(sourceDbType, context, query_dop));
            instance.setTableController(tableController);
            instance.setStatAggregation(statAggregation);
            instance.setTargetDbType(targetDbType);
            instance.setProgressTracer(progressTracer);
            instance.setThreadSize(config.getInt("gauss.extractor.concurrent.size", 300));
            instance.setExecutor(extractorExecutor);
            instances.add(instance);
        }

        logger.info("## prepare start tables[{}] with concurrent[{}]", instances.size(), threadSize);
        int progressPrintInterval = config.getInt("gauss.progress.print.interval", 1);
        schedule = Executors.newScheduledThreadPool(2);
        schedule.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                try {
                    progressTracer.print(true);
                } catch (Throwable e) {
                    logger.error("print progress failed", e);
                }
            }
        }, progressPrintInterval, progressPrintInterval, TimeUnit.MINUTES);
        schedule.execute(new Runnable() {

            @Override
            public void run() {
                while (true) {
                    try {
                        GaussInstance instance = tableController.takeDone();
                        if (instance.isStart()) {
                            instance.stop();
                        }
                    } catch (InterruptedException e) {
                        // do nothging
                        return;
                    } catch (Throwable e) {
                        logger.error("stop failed", e);
                    }
                }
            }
        });

        for (GaussInstance instance : instances) {
            instance.start();
            if (!concurrent) {
                // 如果非并发，则串行等待其返回
                try {
                    instance.waitForDone();
                } catch (Exception e) {
                    logger.error("wait failed", e);
                }

                instance.stop();
            }
        }

        MDC.remove(GaussConstants.MDC_TABLE_SHIT_KEY);
    }

    public void waitForDone() throws InterruptedException {
        tableController.waitForDone();
    }

    @Override
    public void stop() {
        super.stop();
        cleanBeforeEnd();
        for (GaussInstance instance : instances) {
            if (instance.isStart()) {
                instance.stop();
            }
        }
        schedule.shutdownNow();
        MDC.remove(GaussConstants.MDC_TABLE_SHIT_KEY);
        progressTracer.print(true);
        if (dataSourceFactory.isStart()) {
            dataSourceFactory.stop();
        }
        MDC.remove(GaussConstants.MDC_TABLE_SHIT_KEY);
    }

    private RecordExtractor chooseExtractor(GaussContext context) {
        if (sourceDbType == DbType.MYSQL) {
            DbOnceFullRecordExtractor recordExtractor = new DbOnceFullRecordExtractor(context, DbType.MYSQL);
            recordExtractor.setTracer(progressTracer);
            return recordExtractor;
        } else if (sourceDbType == DbType.ORACLE) {
            DbOnceFullRecordExtractor recordExtractor = new DbOnceFullRecordExtractor(context, DbType.ORACLE);
            recordExtractor.setTracer(progressTracer);
            return recordExtractor;
        } else if (sourceDbType == DbType.PG){
            DbOnceFullRecordExtractor recordExtractor = new DbOnceFullRecordExtractor(context, DbType.PG);
            recordExtractor.setTracer(progressTracer);
            return recordExtractor;
        } else {
            throw new GaussException("unsupport " + sourceDbType);
        }
    }

    private RecordApplier chooseApplier(GaussContext context) {
        boolean concurrent = config.getBoolean("gauss.applier.concurrent.enable", true);
        int threadSize = config.getInt("gauss.applier.concurrent.size", 5);
        int splitSize = context.getOnceCrawNum() / threadSize;
        if (splitSize > 100 || splitSize <= 0) {
            splitSize = 100;
        }
        if (concurrent) {
            return new MultiThreadCheckRecordApplier(context, threadSize, splitSize, applierExecutor, query_dop);
        } else {
            return new CheckRecordApplier(context, query_dop);
        }
    }

    private GaussContext buildContext(GaussContext globalContext, Table table) {
        GaussContext result = globalContext.cloneGlobalContext();
        result.setTableMeta(table);
        return result;
    }

    private GaussContext initGlobalContext() {
        GaussContext context = new GaussContext();
        logger.info("check source database connection ...");
        context.setSourceDs(initDataSource("source"));
        logger.info("check source database is ok");

        logger.info("check target database connection ...");
        context.setTargetDs(initDataSource("target"));
        logger.info("check target database is ok");
        context.setSourceEncoding(config.getString("gauss.database.source.encode", "UTF-8"));
        context.setTargetEncoding(config.getString("gauss.database.target.encode", "UTF-8"));
        context.setOnceCrawNum(config.getInt("gauss.table.onceCrawNum", 200));
        context.setTpsLimit(config.getInt("gauss.table.tpsLimit", 2000));
        context.setTablepks(getTablePKs(config.getString("gauss.table.inc.tablepks")));
        return context;
    }

    private Map<String, String[]> getTablePKs(String tablepks) {
        if (StringUtils.isBlank(tablepks)) {
            return null;
        } else {
            Map<String, String[]> tps = new HashMap();
            String[] tables = tablepks.split("\\|");
            for (String table : tables) {
                String[] tablev = table.split("&");
                String tableName = tablev[0];
                String[] pks = new String[tablev.length - 1];
                for (int i = 1; i < tablev.length; i++) {
                    pks[i - 1] = new String(tablev[i]).toString();
                }
                tps.put(new String(tableName).toUpperCase().toString(), pks);
            }
            return tps;
        }
    }

    private DataSource initDataSource(String type) {
        String username = config.getString("gauss.database." + type + ".username");
        String password = config.getString("gauss.database." + type + ".password");
        DbType dbType = DbType.valueOf(config.getString("gauss.database." + type + ".type"));
        String url = config.getString("gauss.database." + type + ".url");
        String encode = config.getString("gauss.database." + type + ".encode");
        String poolSize = config.getString("gauss.database." + type + ".poolSize");

        Properties properties = new Properties();
        if (poolSize != null) {
            properties.setProperty("maxActive", poolSize);
        } else {
            properties.setProperty("maxActive", "200");
        }

        DataSourceConfig dsConfig = new DataSourceConfig(url, username, password, dbType, properties);
        return dataSourceFactory.getDataSource(dsConfig);
    }

    private Collection<TableHolder> initTables() {
        logger.info("check source tables read privileges ...");
        List tableWhiteList = config.getList("gauss.table.white");
        List tableBlackList = config.getList("gauss.table.black");
        boolean isEmpty = true;
        for (Object table : tableWhiteList) {
            isEmpty &= StringUtils.isBlank((String) table);
        }

        List<TableHolder> tables = Lists.newArrayList();
        if (!isEmpty) {
            for (Object obj : tableWhiteList) {
                String whiteTable = getTable((String) obj);
                // 先粗略判断一次
                if (!isBlackTable(whiteTable, tableBlackList)) {
                    String[] strs = StringUtils.split(whiteTable, ".");
                    List<Table> whiteTables = null;
                    if (strs.length == 1) {
                        whiteTables = TableMetaGenerator.getTableMetasWithoutColumn(globalContext.getSourceDs(),
                            strs[0], null);
                    } else if (strs.length == 2) {
                        whiteTables = TableMetaGenerator.getTableMetasWithoutColumn(globalContext.getSourceDs(),
                            strs[0], strs[1]);
                    } else {
                        throw new GaussException("table[" + whiteTable + "] is not valid");
                    }

                    if (whiteTables.isEmpty()) {
                        throw new GaussException("table[" + whiteTable + "] is not found");
                    }

                    for (Table table : whiteTables) {
                        // 根据实际表名处理一下
                        if (!isBlackTable(table.getName(), tableBlackList) && !isBlackTable(table.getFullName(),
                            tableBlackList)) {
                            TableMetaGenerator.buildColumns(globalContext.getSourceDs(), table);
                            TableHolder holder = new TableHolder(table);
                            if (!tables.contains(holder)) {
                                tables.add(holder);
                            }
                        }
                    }
                }
            }
        } else {
            List<Table> metas = TableMetaGenerator.getTableMetasWithoutColumn(globalContext.getSourceDs(), null, null);
            for (Table table : metas) {
                if (!isBlackTable(table.getName(), tableBlackList) && !isBlackTable(table.getFullName(),
                    tableBlackList)) {
                    TableMetaGenerator.buildColumns(globalContext.getSourceDs(), table);
                    TableHolder holder = new TableHolder(table);
                    if (!tables.contains(holder)) {
                        tables.add(holder);
                    }
                }
            }
        }

        logger.info("check source tables is ok.");
        return tables;
    }

    private boolean isBlackTable(String table, List tableBlackList) {
        for (Object tableBlack : tableBlackList) {
            if (LikeUtil.isMatch((String) tableBlack, table)) {
                return true;
            }
        }

        return false;
    }

    private String getTable(String tableName) {
        String[] paramArray = tableName.split("#");
        if (paramArray.length >= 1 && !"".equals(paramArray[0])) {
            return paramArray[0];
        } else {
            return null;
        }
    }

    private void prepareBeforeStart() {
        if (sourceDbType != DbType.ORACLE) {
            return;
        }
        logger.info("create some function to assist checker");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(globalContext.getSourceDs());
        jdbcTemplate.execute("CREATE OR REPLACE FUNCTION datachecker_md5(in_str IN VARCHAR2)" +
                " RETURN VARCHAR2" +
                " IS" +
                " retval varchar2(32);" +
                " BEGIN" +
                " if in_str is not null then" +
                "     retval := lower(utl_raw.cast_to_raw(DBMS_OBFUSCATION_TOOLKIT.MD5(INPUT_STRING => in_str)));" +
                " else" +
                "     retval := null;" +
                " end if;" +
                " RETURN retval;" +
                " END;");
        jdbcTemplate.execute("CREATE OR REPLACE FUNCTION datachecker_get_bfile( p_bfile IN BFILE ) RETURN" +
                " VARCHAR2" +
                "   AS" +
                "         filecontent BLOB := NULL;" +
                "         src_file BFILE := NULL;" +
                "         l_step PLS_INTEGER := 12000;" +
                "         l_dir   VARCHAR2(4000);" +
                "         l_fname VARCHAR2(4000);" +
                "         offset NUMBER := 1;" +
                "   BEGIN" +
                "     IF p_bfile IS NULL THEN" +
                "       RETURN NULL;" +
                "     END IF;" +
                "     DBMS_LOB.FILEGETNAME( p_bfile, l_dir, l_fname );" +
                "     src_file := BFILENAME( l_dir, l_fname );" +
                "     IF src_file IS NULL THEN" +
                "         RETURN NULL;" +
                "     END IF;" +
                "     DBMS_LOB.FILEOPEN(src_file, DBMS_LOB.FILE_READONLY);" +
                "     DBMS_LOB.CREATETEMPORARY(filecontent, true);" +
                "     DBMS_LOB.LOADBLOBFROMFILE (filecontent, src_file, DBMS_LOB.LOBMAXSIZE, offset, offset);" +
                "     DBMS_LOB.FILECLOSE(src_file);" +
                "     return UTL_RAW.CAST_TO_VARCHAR2(filecontent);" +
                " END;");
    }

    private void cleanBeforeEnd() {
        if (sourceDbType != DbType.ORACLE) {
            return;
        }
        logger.info("clean function created by datachecker");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(globalContext.getSourceDs());
        jdbcTemplate.execute("drop function datachecker_md5");
        jdbcTemplate.execute("drop function datachecker_get_bfile");
    }

    private static class TableHolder {

        public TableHolder(Table table) {
            this.table = table;
        }

        Table table;

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((table == null) ? 0 : table.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TableHolder other = (TableHolder) obj;
            if (table == null) {
                if (other.table != null) {
                    return false;
                }
            } else if (!table.equals(other.table)) {
                return false;
            }
            return true;
        }

    }
}

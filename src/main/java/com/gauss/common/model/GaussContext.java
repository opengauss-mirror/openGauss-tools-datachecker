package com.gauss.common.model;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.gauss.common.db.meta.Table;

/**
 * Gauss数据处理上下文
 */
public class GaussContext {

    // 具体的表
    private Table                 tableMeta;                           // 对应的meta

    // 全局共享
    private RunMode               runMode;
    private int                   onceCrawNum;                         // 每次提取的记录数
    private int                   tpsLimit             = 0;            // <=0代表不限制
    private DataSource            sourceDs;                            // 源数据库链接
    private DataSource            targetDs;                            // 目标数据库链接
    private String                sourceEncoding       = "UTF-8";
    private String                targetEncoding       = "UTF-8";

    private Map<String, String[]> tablepks             = new HashMap();

    // 实时同步时指定的判断字段

    public Map<String, String[]> getTablepks() {
        return tablepks;
    }

    public void setTablepks(Map<String, String[]> tablepks) {
        this.tablepks = tablepks;
    }

    public int getOnceCrawNum() {
        return onceCrawNum;
    }
    
    public void setOnceCrawNum(int onceCrawNum) {
        this.onceCrawNum = onceCrawNum;
    }

    public DataSource getSourceDs() {
        return sourceDs;
    }

    public void setSourceDs(DataSource sourceDs) {
        this.sourceDs = sourceDs;
    }

    public DataSource getTargetDs() {
        return targetDs;
    }

    public void setTargetDs(DataSource targetDs) {
        this.targetDs = targetDs;
    }

    public String getSourceEncoding() {
        return sourceEncoding;
    }

    public void setSourceEncoding(String sourceEncoding) {
        this.sourceEncoding = sourceEncoding;
    }

    public String getTargetEncoding() {
        return targetEncoding;
    }

    public void setTargetEncoding(String targetEncoding) {
        this.targetEncoding = targetEncoding;
    }

    public Table getTableMeta() {
        return tableMeta;
    }

    public void setTableMeta(Table tableMeta) {
        this.tableMeta = tableMeta;
    }

    public int getTpsLimit() {
        return tpsLimit;
    }

    public void setTpsLimit(int tpsLimit) {
        this.tpsLimit = tpsLimit;
    }

    public RunMode getRunMode() {
        return runMode;
    }

    public void setRunMode(RunMode runMode) {
        this.runMode = runMode;
    }

    public GaussContext cloneGlobalContext() {
        GaussContext context = new GaussContext();
        context.setRunMode(runMode);
        context.setSourceDs(sourceDs);
        context.setTargetDs(targetDs);
        context.setSourceEncoding(sourceEncoding);
        context.setTargetEncoding(targetEncoding);
        context.setOnceCrawNum(onceCrawNum);
        context.setTpsLimit(tpsLimit);
        context.setTablepks(tablepks);
        return context;
    }

}

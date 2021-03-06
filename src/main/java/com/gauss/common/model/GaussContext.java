package com.gauss.common.model;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.gauss.common.db.meta.Table;

public class GaussContext {

    private Table tableMeta;

    private int onceCrawNum;             // the number of records in one batch

    private int tpsLimit = 0;            // <=0 means there is no limit

    private DataSource sourceDs;         // connection of source database

    private DataSource targetDs;         // connection of target database

    private String sourceEncoding = "UTF-8";

    private String targetEncoding = "UTF-8";

    private Map<String, String[]> tablepks = new HashMap();

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

    public GaussContext cloneGlobalContext() {
        GaussContext context = new GaussContext();
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

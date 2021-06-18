package com.gauss.common.model;

public enum DbType {

    /**
     * mysql DB
     */
    MYSQL("com.mysql.cj.jdbc.Driver"),
    /**
     * openGauss DB
     */
    OPGS("org.postgresql.Driver"),
    /**
     * oracle DB
     */
    ORACLE("oracle.jdbc.driver.OracleDriver"), SqlServer("com.microsoft.sqlserver.jdbc.SQLServerDriver");

    private String driver;

    DbType(String driver){
        this.driver = driver;
    }

    public String getDriver() {
        return driver;
    }

    public boolean isMysql() {
        return this.equals(DbType.MYSQL);
    }

    public boolean isOracle() {
        return this.equals(DbType.ORACLE);
    }

    public boolean isOpenGauss() {
        return this.equals(DbType.OPGS);
    }

}

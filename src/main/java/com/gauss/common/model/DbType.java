package com.gauss.common.model;

public enum DbType {

    /**
     * mysql DB
     */
    MYSQL("com.mysql.cj.jdbc.Driver"),
    /**
     * openGauss DB
     */
    OPGS("org.opengauss.Driver"),
    /**
     * oracle DB
     */
    ORACLE("oracle.jdbc.driver.OracleDriver"),
    /**
     * postgreSQL DB
     */
    PG("org.postgresql.Driver");

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

    public boolean isPostgreSQL() {
        return this.equals(DbType.PG);
    }

}

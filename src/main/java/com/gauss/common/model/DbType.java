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

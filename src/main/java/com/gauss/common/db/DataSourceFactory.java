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

package com.gauss.common.db;

import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.base.Function;
import com.google.common.collect.MigrateMap;
import com.gauss.common.lifecycle.AbstractGaussLifeCycle;
import com.gauss.common.lifecycle.GaussLifeCycle;
import com.gauss.common.model.DataSourceConfig;
import com.gauss.common.model.DbType;
import com.gauss.exception.GaussException;

/**
 * get database connection
 */
public class DataSourceFactory extends AbstractGaussLifeCycle implements GaussLifeCycle {

    private static final Logger               logger      = LoggerFactory.getLogger(DataSourceFactory.class);
    private int                               maxWait     = 5 * 1000;
    private int                               minIdle     = 0;
    private int                               initialSize = 0;
    private int                               maxActive   = 32;

    private Map<DataSourceConfig, DataSource> dataSources;

    public void start() {
        super.start();
        dataSources = MigrateMap.makeComputingMap(new Function<DataSourceConfig, DataSource>() {

            public DataSource apply(DataSourceConfig config) {
                return createDataSource(config.getUrl(),
                    config.getUsername(),
                    config.getPassword(),
                    config.getType(),
                    config.getProperties());
            }
        });

    }

    public void stop() {
        super.stop();

        for (DataSource source : dataSources.values()) {
            DruidDataSource basicDataSource = (DruidDataSource) source;
            basicDataSource.close();
        }

        dataSources.clear();
    }

    public DataSource getDataSource(DataSourceConfig config) {
        return dataSources.get(config);
    }

    public DataSource getDataSource(String url, String userName, String password, DbType dbType, Properties props) {
        return dataSources.get(new DataSourceConfig(url, userName, password, dbType, props));
    }

    private DataSource createDataSource(String url, String userName, String password, DbType dbType, Properties props) {
        try {
            int maxActive = Integer.valueOf(props.getProperty("maxActive", String.valueOf(this.maxActive)));
            if (maxActive < 0) {
                maxActive = 200;
            }
            DruidDataSource dataSource = new DruidDataSource();
            dataSource.setUrl(url);
            dataSource.setUsername(userName);
            dataSource.setPassword(password);
            dataSource.setUseUnfairLock(true);
            dataSource.setNotFullTimeoutRetryCount(2);
            dataSource.setInitialSize(initialSize);
            dataSource.setMinIdle(minIdle);
            dataSource.setMaxActive(maxActive);
            dataSource.setMaxWait(maxWait);
            dataSource.setDriverClassName(dbType.getDriver());

            if (props != null && props.size() > 0) {
                for (Map.Entry<Object, Object> entry : props.entrySet()) {
                    dataSource.addConnectionProperty(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
                }
            }
            return dataSource;
        } catch (Throwable e) {
            throw new GaussException("create dataSource error!", e);
        }
    }

    public void setMaxWait(int maxWait) {
        this.maxWait = maxWait;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    public void setInitialSize(int initialSize) {
        this.initialSize = initialSize;
    }

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

}

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

package com.gauss;

import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gauss.common.utils.GaussUtils;
import com.gauss.controller.GaussController;

public class GaussLauncher {

    private static final String CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger logger               = LoggerFactory.getLogger(GaussLauncher.class);
    private static final Logger summaryLogger    = LoggerFactory.getLogger("summary");

    public static void main(String[] args) throws Throwable {
        try {
            String conf = System.getProperty("gauss.conf", "classpath:gauss.properties");
            PropertiesConfiguration config = new PropertiesConfiguration();
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                config.load(GaussLauncher.class.getClassLoader().getResourceAsStream(conf));
            } else {
                config.load(new FileInputStream(conf));
            }

            logger.info("## start the DataChecker.");
            summaryLogger.info("Start time : " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n");
            final GaussController controller = new GaussController(config);
            controller.start();
            logger.info("## the DataChecker is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    if (controller.isStart()) {
                        try {
                            logger.info("## stop the DataChecker");
                            controller.stop();
                        } catch (Throwable e) {
                            logger.warn("## something goes wrong when stopping DataChecker:\n{}",
                                ExceptionUtils.getFullStackTrace(e));
                        } finally {
                            summaryLogger.info("End time : " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n");
                            logger.info("## DataChecker is down.");
                        }
                    }
                }

            });

            controller.waitForDone();// 如果所有都完成，则进行退出
            Thread.sleep(3 * 1000); // 等待3s，清理上下文
            logger.info("## stop the DataChecker");
            if (controller.isStart()) {
                controller.stop();
            }
            summaryLogger.info("End time : " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n");
            logger.info("## DataChecker is down.");
            logger.info("Please check log for more information.");
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the DataChecker:\n{}",
                ExceptionUtils.getFullStackTrace(e));
            summaryLogger.info("End time : " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n");
            System.exit(0);
        }
    }
}

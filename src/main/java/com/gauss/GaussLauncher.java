package com.gauss;

import java.io.FileInputStream;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gauss.controller.GaussController;

public class GaussLauncher {

    private static final String CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger logger               = LoggerFactory.getLogger(GaussLauncher.class);

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
            logger.info("## DataChecker is down.");
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the DataChecker:\n{}",
                ExceptionUtils.getFullStackTrace(e));
            System.exit(0);
        }
    }
}

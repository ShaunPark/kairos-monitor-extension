/**
 * Copyright 2016 Solulink co ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kr.co.soluink.appdynamics.extension;

import com.appdynamics.extensions.PathResolver;
import com.appdynamics.extensions.yml.YmlReader;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;

import kr.co.soluink.appdynamics.extension.config.Configuration;
import kr.co.soluink.appdynamics.extension.query.Query;

import org.apache.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class KairosMonitor extends AManagedMonitor {
    private static final Logger logger = Logger.getLogger(KairosMonitor.class);
    private static final String CONFIG_ARG = "config-file";
    private Map<String, String> valueMap;

    public KairosMonitor() throws ClassNotFoundException {
        String msg = "Using Monitor Version [" + getImplementationVersion() + "]";
        System.out.println(msg);
        Class.forName("kr.co.realtimetech.kairos.jdbc.kairosDriver");
    }

    public TaskOutput execute(Map<String, String> taskArguments, TaskExecutionContext taskContext)
            throws TaskExecutionException {
        if(taskArguments != null) {
            logger.info("Starting " + getImplementationVersion() + " Monitoring Task");
            try {
                String configFilename = getConfigFilename(taskArguments.get(CONFIG_ARG));
                Configuration config = YmlReader.readFromFile(configFilename, Configuration.class);

                fetchMetrics(config, Query.queries);

                printDBMetrics(config);

                logger.info("Kairos DB Monitoring Task completed successfully");
                return new TaskOutput("Kairos DB Monitoring Task completed successfully");
            } catch (Exception e) {
                logger.error("Metrics Collection Failed: ", e);
            }
        }
        throw new TaskExecutionException("Kairos DB Monitoring Task completed with failures.");
    }

    private void fetchMetrics(Configuration config, String[] queries) throws Exception {
        valueMap = Maps.newHashMap();
        Connection conn = null;
        Statement stmt = null;
        boolean debug = logger.isDebugEnabled();
        try {
            conn = connect(config);
            stmt = conn.createStatement();
            for (String query : queries) {
                ResultSet rs = null;
                try {
                    if (debug) {
                        logger.debug("Executing query [" + query + "]");
                    }
                    rs = stmt.executeQuery(query);
                    while (rs.next()) {
                        String key = rs.getString(1);
                        String value = rs.getString(2);
                        if (debug) {
                            logger.debug("[key,value] = [" + key + "," + value + "]");
                        }
                        valueMap.put(key.toUpperCase(), value);
                    }
                } catch (Exception ex) {
                    logger.error("Error while executing query [" + query + "]", ex);
                    throw ex;
                } finally {
                    close(rs, null, null);
                }
            }
        } finally {
            close(null, stmt, conn);
        }
    }

    private Connection connect(Configuration config) throws SQLException {
        String host = config.getHost();
        String port = String.valueOf(config.getPort());
        String userName = config.getUsername();
        String password = config.getPassword();
        String dbname = config.getDbName();

        if(Strings.isNullOrEmpty(port)) {
            port = "5000";
        }
        if(Strings.isNullOrEmpty(dbname)) {
        	dbname = "test";
        }
        String connStr = String.format("jdbc:kairos://%s:%s/%s", host, port, dbname);
        logger.debug("Connecting to: " + connStr);

        Connection conn = DriverManager.getConnection(connStr, userName, password);
        logger.debug("Successfully connected to Kairos DB");

        return conn;
    }

    private void printDBMetrics(Configuration config) {
        String metricPath = config.getMetricPathPrefix() + config.getDbName() + "|";
        // RESOURCE UTILIZATION ////////////////////////////////////
        String resourceUtilizationMetricPath = metricPath + "Resource Utilization|";
        for( int i = 0 ; i < Query.utilizationKeys.length ; i++ ) {
        	printMetric(resourceUtilizationMetricPath + Query.utilizationTitles[i], getString(Query.utilizationKeys[i]));
        }

        // ACTIVITY ////////////////////////////////////////////////
        String activityMetricPath = metricPath + "Activity|";
        for( int i = 0 ; i < Query.activityKeys.length ; i++ ) {
        	printMetric(activityMetricPath + Query.activityTitles[i], getString(Query.activityKeys[i]));
        }
        

        // EFFICIENCY //////////////////////////////////////////////
        String efficiencyMetricPath = metricPath + "Efficiency|";
        for( int i = 0 ; i < Query.effiencyKeys.length ; i++ ) {
        	printMetric(efficiencyMetricPath + Query.effiencyTitles[i], getString(Query.effiencyKeys[i]));
        }
    }

    protected void printMetric(String metricName, String value) {
        if(!Strings.isNullOrEmpty(value)) {
            try {
                MetricWriter metricWriter = getMetricWriter(metricName, MetricWriter.METRIC_AGGREGATION_TYPE_AVERAGE, MetricWriter.METRIC_TIME_ROLLUP_TYPE_AVERAGE,
                        MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_INDIVIDUAL);
                metricWriter.printMetric(value);
                //System.out.println(metricName + "  " + value);
                if (logger.isDebugEnabled()) {
                    logger.debug("METRIC:  NAME:" + metricName + " VALUE:" + value);
                }
            } catch (Exception e) {
                logger.error(e);
            }
        }
    }

    protected void close(ResultSet rs, Statement stmt, Connection conn) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                // ignore
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (Exception e) {
                // ignore
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    protected String getString(float num) {
        int result = Math.round(num);
        return Integer.toString(result);
    }

    // lookup value for key, convert to float, round up or down and then return as string form of int
    protected String getString(String key) {
        return getString(key, true);
    }

    // specify whether to convert this key to uppercase before looking up the value
    protected String getString(String key, boolean convertUpper) {
        if (convertUpper)
            key = key.toUpperCase();
        String strResult = valueMap.get(key);
        if (strResult == null) {
            return "";
        }
        // round the result to a integer since we don't handle fractions
        float result = Float.valueOf(strResult);
        String resultStr = getString(result);
        return resultStr;
    }

    private String getConfigFilename(String filename) {
        if (filename == null) {
            return "";
        }
        //for absolute paths
        if (new File(filename).exists()) {
            return filename;
        }
        //for relative paths
        File jarPath = PathResolver.resolveDirectory(AManagedMonitor.class);
        String configFileName = "";
        if (!Strings.isNullOrEmpty(filename)) {
            configFileName = jarPath + File.separator + filename;
        }
        return configFileName;
    }

    public static String getImplementationVersion() {
        return KairosMonitor.class.getPackage().getImplementationTitle();
    }

    public static void main(String[] args) {
        System.out.println("Using Monitor Version [" + getImplementationVersion() + "]");
    }
}
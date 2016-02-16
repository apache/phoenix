/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf;

import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PherfConstants {
    public static enum GeneratePhoenixStats {
        YES,
        NO
    }
    
    public static enum CompareType {
        MINIMUM,
        AVERAGE
    }

    private static PherfConstants instance = null;
    private static Properties instanceProperties = null;

    public static final int DEFAULT_THREAD_POOL_SIZE = 10;
    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final DateTimeZone DEFAULT_TIME_ZONE = DateTimeZone.UTC;
    public static final String RESOURCE_SCENARIO = "/scenario";
    public static final String
            SCENARIO_ROOT_PATTERN =
            		".*" + PherfConstants.RESOURCE_SCENARIO.substring(1) + ".*" + PherfConstants.RESOURCE_SCENARIO.substring(1) + ".*";
    public static final String SCHEMA_ROOT_PATTERN = ".*";
    public static final String TEST_SCENARIO_ROOT_PATTERN = ".*" + "test" + "_" + PherfConstants.RESOURCE_SCENARIO.substring(1);
    public static final String PHERF_PROPERTIES = "pherf.properties";

    public static final String EXPORT_DIR = "CSV_EXPORT";
    public static final String RESULT_PREFIX = "RESULT_";
    public static final String PATH_SEPARATOR = "/";
    public static final String RESULT_FILE_DELIMETER = ",";
    public static final String NEW_LINE = "\n";

    public static final long DEFAULT_NUMBER_OF_EXECUTIONS = 10;
    public static final long DEFAULT_THREAD_DURATION_IN_MS = 10000;
    public static final String DEFAULT_CONCURRENCY = "1";

    public static final String DIFF_PASS = "VERIFIED_DIFF";
    public static final String DIFF_FAIL = "FAILED_DIFF";

    public static final String PHERF_SCHEMA_NAME = "PHERF";

    // TODO MOve to properties
    // log out data load per n rows
    public static final int LOG_PER_NROWS = 1000000;
    public static final String COMBINED_FILE_NAME = "COMBINED";

    public static final String EXPORT_TMP = EXPORT_DIR + "_TMP";
    public static final String RESOURCE_DATAMODEL = "/datamodel";

    // Default frequency in ms in which to log out monitor stats
    public static final int MONITOR_FREQUENCY = 5000;
    public static final String MONITOR_FILE_NAME = "STATS_MONITOR";

    private PherfConstants() {
    }

    public static PherfConstants create() {
        if (instance == null) {
            instance = new PherfConstants();
        }
        return instance;
    }

    /**
     * Get a {@link Properties} object based on the file name
     * @param fileName      Name of the file
     * @param getDefault    True if you want to use the properties that may have been loaded into
     *                      the instance. use false if you want to reload the passed file.
     * @return {@link Properties}
     * @throws Exception
     */
    public Properties getProperties(final String fileName, boolean getDefault) throws Exception {

        if (instanceProperties == null) {
            instanceProperties = loadProperties(fileName);
        } else {
            return getDefault ? instanceProperties : loadProperties(fileName);
        }

        return instanceProperties;
    }

    private Properties loadProperties(String fileName) throws IOException{
        Properties properties = new Properties();
        InputStream is = null;
        try {
            is = getClass().getClassLoader().getResourceAsStream(fileName);
            if (is != null) {
                properties.load(is);
            }
        } finally {
            if (is != null) {
                is.close();
            }
        }
        return properties;
    }

    public String getProperty(String property) {
        return getProperty(PherfConstants.PHERF_PROPERTIES, property);
    }

    public String getProperty(final String fileName, String property) {
        String value = null;
        try {
            value = getProperties(fileName, false).getProperty(property);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }
}

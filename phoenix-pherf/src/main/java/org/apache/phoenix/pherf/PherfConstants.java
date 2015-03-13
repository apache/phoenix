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

public class PherfConstants {
    public static final int DEFAULT_THREAD_POOL_SIZE = 10;
    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String DEFAULT_FILE_PATTERN = ".*scenario.xml";
    public static final String RESOURCE_SCENARIO = "/scenario";
    public static final String
            SCENARIO_ROOT_PATTERN =
            ".*" + PherfConstants.RESOURCE_SCENARIO.substring(1) + ".*";
    public static final String SCHEMA_ROOT_PATTERN = ".*";
    public static final String PHERF_PROPERTIES = "pherf.properties";
    public static final String RESULT_DIR = "RESULTS";
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

    // log out data load per n rows
    public static final int LOG_PER_NROWS = 1000000;
    public static final String COMBINED_FILE_NAME = "COMBINED";

    public static final String EXPORT_TMP = EXPORT_DIR + "_TMP";
    public static final String RESOURCE_DATAMODEL = "/datamodel";

    // Default frequency in ms in which to log out monitor stats
    public static final int MONITOR_FREQUENCY = 5000;
    public static final String MONITOR_FILE_NAME = "STATS_MONITOR";

    public static enum RunMode {
        PERFORMANCE,
        FUNCTIONAL
    }
}

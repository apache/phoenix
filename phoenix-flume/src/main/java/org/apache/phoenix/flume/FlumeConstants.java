/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.flume;

public final class FlumeConstants {

    /**
     * The Hbase table which the sink should write to.
     */
    public static final String CONFIG_TABLE = "table";
    /**
     * The ddl query for the Hbase table where events are ingested to.
     */
    public static final String CONFIG_TABLE_DDL = "ddl";
    /**
     * Maximum number of events the sink should take from the channel per transaction, if available.
     */
    public static final String CONFIG_BATCHSIZE = "batchSize";
    /**
     * The fully qualified class name of the serializer the sink should use.
     */
    public static final String CONFIG_SERIALIZER = "serializer";
    /**
     * Configuration to pass to the serializer.
     */
    public static final String CONFIG_SERIALIZER_PREFIX = CONFIG_SERIALIZER + ".";

    /**
     * Configuration for the zookeeper quorum.
     */
    public static final String CONFIG_ZK_QUORUM = "zookeeperQuorum";
    
    /**
     * Configuration for the jdbc url.
     */
    public static final String CONFIG_JDBC_URL = "jdbcUrl";

    /**
     * Default batch size .
     */
    public static final Integer DEFAULT_BATCH_SIZE = 100;

    /** Regular expression used to parse groups from event data. */
    public static final String CONFIG_REGULAR_EXPRESSION = "regex";
    public static final String REGEX_DEFAULT = "(.*)";

    /** Whether to ignore case when performing regex matches. */
    public static final String IGNORE_CASE_CONFIG = "regexIgnoreCase";
    public static final boolean IGNORE_CASE_DEFAULT = false;

    /** Comma separated list of column names . */
    public static final String CONFIG_COLUMN_NAMES = "columns";

    /** The header columns to persist as columns into the default column family. */
    public static final String CONFIG_HEADER_NAMES = "headers";

    /** The rowkey type generator . */
    public static final String CONFIG_ROWKEY_TYPE_GENERATOR = "rowkeyType";

    /**
     * The default delimiter for columns and headers
     */
    public static final String DEFAULT_COLUMNS_DELIMITER = ",";
}

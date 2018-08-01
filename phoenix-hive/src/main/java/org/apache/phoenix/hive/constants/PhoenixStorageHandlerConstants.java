/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive.constants;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;

import java.util.List;

/**
 * Constants using for Hive Storage Handler implementation
 */
public class PhoenixStorageHandlerConstants {

    public static final String HBASE_INPUT_FORMAT_CLASS = "phoenix.input.format.class";

    public static final String PHOENIX_TABLE_NAME = "phoenix.table.name";

    public static final String DEFAULT_PHOENIX_INPUT_CLASS = "org.apache.phoenix.hive.mapreduce" +
            ".PhoenixResultWritable";

    public static final String ZOOKEEPER_QUORUM = "phoenix.zookeeper.quorum";
    public static final String ZOOKEEPER_PORT = "phoenix.zookeeper.client.port";
    public static final String ZOOKEEPER_PARENT = "phoenix.zookeeper.znode.parent";
    public static final String DEFAULT_ZOOKEEPER_QUORUM = "localhost";
    public static final int DEFAULT_ZOOKEEPER_PORT = 2181;
    public static final String DEFAULT_ZOOKEEPER_PARENT = "/hbase";

    public static final String PHOENIX_ROWKEYS = "phoenix.rowkeys";
    public static final String PHOENIX_COLUMN_MAPPING = "phoenix.column.mapping";
    public static final String PHOENIX_TABLE_OPTIONS = "phoenix.table.options";

    public static final String PHOENIX_TABLE_QUERY_HINT = ".query.hint";
    public static final String PHOENIX_REDUCER_NUMBER = ".reducer.count";
    public static final String DISABLE_WAL = ".disable.wal";
    public static final String BATCH_MODE = "batch.mode";
    public static final String AUTO_FLUSH = ".auto.flush";

    public static final String COLON = ":";
    public static final String COMMA = ",";
    public static final String EMPTY_STRING = "";
    public static final String SPACE = " ";
    public static final String LEFT_ROUND_BRACKET = "(";
    public static final String RIGHT_ROUND_BRACKET = ")";
    public static final String QUOTATION_MARK = "'";
    public static final String EQUAL = "=";
    public static final String IS = "is";
    public static final String QUESTION = "?";

    public static final String SPLIT_BY_STATS = "split.by.stats";
    public static final String HBASE_SCAN_CACHE = "hbase.scan.cache";
    public static final String HBASE_SCAN_CACHEBLOCKS = "hbase.scan.cacheblock";
    public static final String HBASE_DATE_FORMAT = "hbase.date.format";
    public static final String HBASE_TIMESTAMP_FORMAT = "hbase.timestamp.format";
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    public static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    public static final String IN_OUT_WORK = "in.out.work";
    public static final String IN_WORK = "input";
    public static final String OUT_WORK = "output";

    public static final String MR = "mr";
    public static final String TEZ = "tez";
    public static final String SPARK = "spark";

    public static final String DATE_TYPE = "date";
    public static final String TIMESTAMP_TYPE = "timestamp";
    public static final String BETWEEN_COMPARATOR = "between";
    public static final String IN_COMPARATOR = "in";
    public static final List<String> COMMON_COMPARATOR = Lists.newArrayList("=", "<", ">", "<=",
            ">=");

    // date/timestamp
    public static final String COLUMNE_MARKER = "$columnName$";
    public static final String PATERN_MARKER = "$targetPattern$";
    public static final String DATE_PATTERN = "'?\\d{4}-\\d{2}-\\d{2}'?";
    public static final String TIMESTAMP_PATTERN = "'?\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\" +
            ".?\\d{0,9}'?";
    public static final String COMMON_OPERATOR_PATTERN = "(\\(?\"?" + COLUMNE_MARKER + "\"?\\)?\\s*" +
            "(=|>|<|<=|>=)\\s*(" + PATERN_MARKER + "))";
    public static final String BETWEEN_OPERATOR_PATTERN = "(\\(?\"?" + COLUMNE_MARKER + "\"?\\)?\\s*(" +
            "(?i)not)?\\s*(?i)between\\s*(" + PATERN_MARKER + ")\\s*(?i)and\\s*(" + PATERN_MARKER
            + "))";
    public static final String IN_OPERATOR_PATTERN = "(\\(?\"?" + COLUMNE_MARKER + "\"?\\)?\\s*((?i)" +
            "not)?\\s*(?i)in\\s*\\((" + PATERN_MARKER + ",?\\s*)+\\))";

    public static final String FUNCTION_VALUE_MARKER = "$value$";
    public static final String DATE_FUNCTION_TEMPLETE = "to_date(" + FUNCTION_VALUE_MARKER + ")";
    public static final String TIMESTAMP_FUNCTION_TEMPLATE = "TIMESTAMP" +
            FUNCTION_VALUE_MARKER;

    public static final IntWritable INT_ZERO = new IntWritable(0);
}

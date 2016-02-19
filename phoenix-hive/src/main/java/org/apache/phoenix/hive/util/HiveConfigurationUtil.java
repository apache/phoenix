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
package org.apache.phoenix.hive.util;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.PhoenixRuntime;

/**
 *
 */
public class HiveConfigurationUtil {
    static Log LOG = LogFactory.getLog(HiveConfigurationUtil.class.getName());

    public static final String TABLE_NAME = "phoenix.hbase.table.name";
    public static final String ZOOKEEPER_QUORUM = "phoenix.zookeeper.quorum";
    public static final String ZOOKEEPER_PORT = "phoenix.zookeeper.client.port";
    public static final String ZOOKEEPER_PARENT = "phoenix.zookeeper.znode.parent";
    public static final String ZOOKEEPER_QUORUM_DEFAULT = "localhost";
    public static final String ZOOKEEPER_PORT_DEFAULT = "2181";
    public static final String ZOOKEEPER_PARENT_DEFAULT = "/hbase-unsecure";

    public static final String COLUMN_MAPPING = "phoenix.column.mapping";
    public static final String AUTOCREATE = "autocreate";
    public static final String AUTODROP = "autodrop";
    public static final String AUTOCOMMIT = "autocommit";
    public static final String PHOENIX_ROWKEYS = "phoenix.rowkeys";
    public static final String SALT_BUCKETS = "saltbuckets";
    public static final String COMPRESSION = "compression";
    public static final String VERSIONS = "versions";
    public static final int VERSIONS_NUM = 5;
    public static final String SPLIT = "split";
    public static final String REDUCE_SPECULATIVE_EXEC =
            "mapred.reduce.tasks.speculative.execution";
    public static final String MAP_SPECULATIVE_EXEC = "mapred.map.tasks.speculative.execution";

    public static void setProperties(Properties tblProps, Map<String, String> jobProperties) {
        String quorum = tblProps.getProperty(HiveConfigurationUtil.ZOOKEEPER_QUORUM) != null ?
                        tblProps.getProperty(HiveConfigurationUtil.ZOOKEEPER_QUORUM) :
                        HiveConfigurationUtil.ZOOKEEPER_QUORUM_DEFAULT;
        String znode = tblProps.getProperty(HiveConfigurationUtil.ZOOKEEPER_PARENT) != null ?
                        tblProps.getProperty(HiveConfigurationUtil.ZOOKEEPER_PARENT) :
                        HiveConfigurationUtil.ZOOKEEPER_PARENT_DEFAULT;
        String port = tblProps.getProperty(HiveConfigurationUtil.ZOOKEEPER_PORT) != null ?
                        tblProps.getProperty(HiveConfigurationUtil.ZOOKEEPER_PORT) :
                        HiveConfigurationUtil.ZOOKEEPER_PORT_DEFAULT;
        if (!znode.startsWith("/")) {
            znode = "/" + znode;
        }
        LOG.debug("quorum:" + quorum);
        LOG.debug("port:" + port);
        LOG.debug("parent:" +znode);
        LOG.debug("table:" + tblProps.getProperty(HiveConfigurationUtil.TABLE_NAME));
        LOG.debug("batch:" + tblProps.getProperty(PhoenixConfigurationUtil.UPSERT_BATCH_SIZE));
        
        jobProperties.put(HiveConfigurationUtil.ZOOKEEPER_QUORUM, quorum);
        jobProperties.put(HiveConfigurationUtil.ZOOKEEPER_PORT, port);
        jobProperties.put(HiveConfigurationUtil.ZOOKEEPER_PARENT, znode);
        String tableName = tblProps.getProperty(HiveConfigurationUtil.TABLE_NAME);
        if (tableName == null) {
            tableName = tblProps.get("name").toString();
            tableName = tableName.split(".")[1];
        }
        // TODO this is sync with common Phoenix mechanism revisit to make wiser decisions
        jobProperties.put(HConstants.ZOOKEEPER_QUORUM,quorum+PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR
                + port + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR+znode);
        jobProperties.put(HiveConfigurationUtil.TABLE_NAME, tableName);
        // TODO this is sync with common Phoenix mechanism revisit to make wiser decisions
        jobProperties.put(PhoenixConfigurationUtil.OUTPUT_TABLE_NAME, tableName);
        jobProperties.put(PhoenixConfigurationUtil.INPUT_TABLE_NAME, tableName);
    }
}

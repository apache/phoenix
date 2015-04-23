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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.util.QueryUtil;

import com.google.common.base.Preconditions;

/**
 * Created by nmaillard on 6/23/14.
 */
public class PhoenixHiveConfiguration {
    private static final Log LOG = LogFactory.getLog(PhoenixHiveConfiguration.class);
    private PhoenixHiveConfigurationUtil util;
    private final Configuration conf = null;

    private String Quorum = HiveConfigurationUtil.ZOOKEEPER_QUORUM_DEFAULT;
    private String Port = HiveConfigurationUtil.ZOOKEEPER_PORT_DEFAULT;
    private String Parent = HiveConfigurationUtil.ZOOKEEPER_PARENT_DEFAULT;
    private String TableName;
    private String DbName;
    private long BatchSize = PhoenixConfigurationUtil.DEFAULT_UPSERT_BATCH_SIZE;

    public PhoenixHiveConfiguration(Configuration conf) {
        // this.conf = conf;
        this.util = new PhoenixHiveConfigurationUtil();
    }

    public PhoenixHiveConfiguration(Table tbl) {
        Map<String, String> mps = tbl.getParameters();
        String quorum =
                mps.get(HiveConfigurationUtil.ZOOKEEPER_QUORUM) != null ? mps
                        .get(HiveConfigurationUtil.ZOOKEEPER_QUORUM)
                        : HiveConfigurationUtil.ZOOKEEPER_QUORUM_DEFAULT;
        String port =
                mps.get(HiveConfigurationUtil.ZOOKEEPER_PORT) != null ? mps
                        .get(HiveConfigurationUtil.ZOOKEEPER_PORT)
                        : HiveConfigurationUtil.ZOOKEEPER_PORT_DEFAULT;
        String parent =
                mps.get(HiveConfigurationUtil.ZOOKEEPER_PARENT) != null ? mps
                        .get(HiveConfigurationUtil.ZOOKEEPER_PARENT)
                        : HiveConfigurationUtil.ZOOKEEPER_PARENT_DEFAULT;
        String pk = mps.get(HiveConfigurationUtil.PHOENIX_ROWKEYS);
        if (!parent.startsWith("/")) {
            parent = "/" + parent;
        }
        String tablename =
                (mps.get(HiveConfigurationUtil.TABLE_NAME) != null) ? mps
                        .get(HiveConfigurationUtil.TABLE_NAME) : tbl.getTableName();

        String mapping = mps.get(HiveConfigurationUtil.COLUMN_MAPPING);
    }

    public void configure(String server, String tableName, long batchSize) {
        // configure(server, tableName, batchSize, null);
    }

    public void configure(String quorum, String port, String parent, long batchSize,
            String tableName, String dbname, String columns) {
        Quorum = quorum != null ? quorum : HiveConfigurationUtil.ZOOKEEPER_QUORUM_DEFAULT;
        Port = port != null ? port : HiveConfigurationUtil.ZOOKEEPER_PORT_DEFAULT;
        Parent = parent != null ? parent : HiveConfigurationUtil.ZOOKEEPER_PARENT_DEFAULT;
        // BatchSize = batchSize!=null?batchSize:ConfigurationUtil.DEFAULT_UPSERT_BATCH_SIZE;
    }

    static class PhoenixHiveConfigurationUtil {

        public Connection getConnection(final Configuration configuration) throws SQLException {
            Preconditions.checkNotNull(configuration);
            Properties props = new Properties();
            // final Connection conn =
            // DriverManager.getConnection(QueryUtil.getUrl(configuration.get(SERVER_NAME)),props).unwrap(PhoenixConnection.class);
            // conn.setAutoCommit(false);
            return null;
        }
    }

}
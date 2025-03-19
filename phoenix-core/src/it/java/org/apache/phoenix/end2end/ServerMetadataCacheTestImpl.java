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
package org.apache.phoenix.end2end;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.phoenix.cache.ServerMetadataCacheImpl;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Implementation of {@link ServerMetadataCache} for Integration Tests.
 * Supports keeping more than one instanceof the cache keyed on the regionserver ServerName.
 *
 * PhoenixRegionServerEndpoint is a region server coproc. There is a 1-1 correspondence between
 * PhoenixRegionServerEndpoint and ServerMetadataCache. In ITs we can have multiple regionservers
 * per cluster so we need multiple instances of ServerMetadataCache in the same jvm. Tests using
 * HighAvailabilityTestingUtility create 2 clusters so we need to have one instance of
 * ServerMetadataCache for each regionserver in each cluster.
 */
public class ServerMetadataCacheTestImpl extends ServerMetadataCacheImpl {
    private static volatile Map<ServerName, ServerMetadataCacheTestImpl> INSTANCES = new HashMap<>();
    private Connection connectionForTesting;

    ServerMetadataCacheTestImpl(Configuration conf) {
        super(conf);
    }

    public static ServerMetadataCacheTestImpl getInstance(Configuration conf, ServerName serverName) {
        ServerMetadataCacheTestImpl result = INSTANCES.get(serverName);
        if (result == null) {
            synchronized (ServerMetadataCacheTestImpl.class) {
                result = INSTANCES.get(serverName);
                if (result == null) {
                    result = new ServerMetadataCacheTestImpl(conf);
                    INSTANCES.put(serverName, result);
                }
            }
        }
        return result;
    }

    public static void setInstance(ServerName serverName, ServerMetadataCacheTestImpl cache) {
        INSTANCES.put(serverName, cache);
    }

    public Long getLastDDLTimestampForTableFromCacheOnly(byte[] tenantID, byte[] schemaName,
                                                         byte[] tableName) {
        byte[] tableKey = SchemaUtil.getTableKey(tenantID, schemaName, tableName);
        ImmutableBytesPtr tableKeyPtr = new ImmutableBytesPtr(tableKey);
        return lastDDLTimestampMap.getIfPresent(tableKeyPtr);
    }

    public void setConnectionForTesting(Connection connection) {
        this.connectionForTesting = connection;
    }

    public static void resetCache() {
        INSTANCES.clear();
    }

    @Override
    protected Connection getConnection(Properties properties) throws SQLException {
        return connectionForTesting != null ? connectionForTesting
                : super.getConnection(properties);
    }
}

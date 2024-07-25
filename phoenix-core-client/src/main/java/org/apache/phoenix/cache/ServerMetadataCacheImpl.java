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
package org.apache.phoenix.cache;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.metrics.MetricsMetadataCachingSource;
import org.apache.phoenix.coprocessorclient.metrics.MetricsPhoenixCoprocessorSourceFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.phoenix.thirdparty.com.google.common.cache.RemovalListener;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
/**
 * This manages the cache for all the objects(data table, views, indexes) on each region server.
 * Currently, it only stores LAST_DDL_TIMESTAMP in the cache.
 */
public class ServerMetadataCacheImpl implements ServerMetadataCache {

    protected Configuration conf;
    // key is the combination of <tenantID, schema name, table name>, value is the lastDDLTimestamp
    protected final Cache<ImmutableBytesPtr, Long> lastDDLTimestampMap;
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerMetadataCacheImpl.class);
    private static final String PHOENIX_COPROC_REGIONSERVER_CACHE_SIZE
            = "phoenix.coprocessor.regionserver.cache.size";
    private static final long DEFAULT_PHOENIX_COPROC_REGIONSERVER_CACHE_SIZE = 10000L;
    private static volatile ServerMetadataCacheImpl cacheInstance;
    private MetricsMetadataCachingSource metricsSource;

    /**
     * Creates/gets an instance of ServerMetadataCache.
     *
     * @param conf configuration
     * @return cache
     */
    public static ServerMetadataCacheImpl getInstance(Configuration conf) {
        ServerMetadataCacheImpl result = cacheInstance;
        if (result == null) {
            synchronized (ServerMetadataCacheImpl.class) {
                result = cacheInstance;
                if (result == null) {
                    cacheInstance = result = new ServerMetadataCacheImpl(conf);
                }
            }
        }
        return result;
    }

    public ServerMetadataCacheImpl(Configuration conf) {
        this.conf = HBaseConfiguration.create(conf);
        this.metricsSource = MetricsPhoenixCoprocessorSourceFactory
                                .getInstance().getMetadataCachingSource();
        long maxSize = conf.getLong(PHOENIX_COPROC_REGIONSERVER_CACHE_SIZE,
                DEFAULT_PHOENIX_COPROC_REGIONSERVER_CACHE_SIZE);
        lastDDLTimestampMap = CacheBuilder.newBuilder()
                .removalListener((RemovalListener<ImmutableBytesPtr, Long>) notification -> {
                    String key = notification.getKey().toString();
                    LOGGER.debug("Expiring " + key + " because of "
                            + notification.getCause().name());
                })
                // maximum number of entries this cache can handle.
                .maximumSize(maxSize)
                .build();
    }

    /**
     * Returns the last DDL timestamp from the table.
     * If not found in cache, then query SYSCAT regionserver.
     * @param tenantID tenant id
     * @param schemaName schema name
     * @param tableName table name
     * @return last DDL timestamp
     * @throws Exception
     */
    public long getLastDDLTimestampForTable(byte[] tenantID, byte[] schemaName, byte[] tableName)
            throws SQLException {
        byte[] tableKey = SchemaUtil.getTableKey(tenantID, schemaName, tableName);
        ImmutableBytesPtr tableKeyPtr = new ImmutableBytesPtr(tableKey);
        // Lookup in cache if present.
        Long lastDDLTimestamp = lastDDLTimestampMap.getIfPresent(tableKeyPtr);
        if (lastDDLTimestamp != null) {
            metricsSource.incrementRegionServerMetadataCacheHitCount();
            LOGGER.trace("Retrieving last ddl timestamp value from cache for " + "schema: {}, " +
                    "table: {}", Bytes.toString(schemaName), Bytes.toString(tableName));
            return lastDDLTimestamp;
        }
        metricsSource.incrementRegionServerMetadataCacheMissCount();
        PTable table;
        String tenantIDStr = Bytes.toString(tenantID);
        if (tenantIDStr == null || tenantIDStr.isEmpty()) {
            tenantIDStr = null;
        }
        Properties properties = new Properties();
        if (tenantIDStr != null) {
            properties.setProperty(TENANT_ID_ATTRIB, tenantIDStr);
        }
        try (Connection connection = getConnection(properties)) {
            // Using PhoenixConnection#getTableFromServerNoCache to completely bypass CQSI cache.
            table = connection.unwrap(PhoenixConnection.class)
                    .getTableFromServerNoCache(schemaName, tableName);
            // TODO PhoenixConnection#getTableFromServerNoCache can throw TableNotFoundException.
            //  In that case, do we want to throw non retryable exception back to the client?
            // Update cache with the latest DDL timestamp from SYSCAT server.
            lastDDLTimestampMap.put(tableKeyPtr, table.getLastDDLTimestamp());
        }
        return table.getLastDDLTimestamp();
    }

    /**
     * Invalidate cache for the given tenantID, schema name and table name.
     * Guava cache is thread safe so we don't have to synchronize it explicitly.
     * @param tenantID tenantID
     * @param schemaName schemaName
     * @param tableName tableName
     */
    public void invalidate(byte[] tenantID, byte[] schemaName, byte[] tableName) {
        LOGGER.info("Invalidating server metadata cache for tenantID: {}, schema: {},  table: {}",
                Bytes.toString(tenantID), Bytes.toString(schemaName), Bytes.toString(tableName));
        byte[] tableKey = SchemaUtil.getTableKey(tenantID, schemaName, tableName);
        ImmutableBytesPtr tableKeyPtr = new ImmutableBytesPtr(tableKey);
        lastDDLTimestampMap.invalidate(tableKeyPtr);
    }

    protected Connection getConnection(Properties properties) throws SQLException {
        return QueryUtil.getConnectionOnServer(properties, this.conf);
    }
}

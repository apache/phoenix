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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.DDLTimestampMaintainersProtos;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.phoenix.thirdparty.com.google.common.cache.RemovalListener;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;

/**
 * This manages the cache for all the objects(data table, views, indexes) on each region server.
 * Currently, it only stores LAST_DDL_TIMESTAMP in the cache.
 */
public class ServerMetadataCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerMetadataCache.class);
    private static final String PHOENIX_COPROC_REGIONSERVER_CACHE_TTL_MS = "phoenix.coprocessor.regionserver.cahe.ttl.ms";
    // Keeping default cache expiry for 30 mins since we won't have stale entry for more than 30 mins.
    private static final long DEFAULT_PHOENIX_COPROC_REGIONSERVER_CACHE_TTL_MS = 30 * 60 * 1000; // 30 mins
    private static volatile ServerMetadataCache INSTANCE;
    private Configuration conf;
    // key is the combination of <tenantID, schema name, table name>, value is the lastDDLTimestamp
    private final Cache<ImmutableBytesPtr, Long> lastDDLTimestampMap;

    /**
     * Creates/gets an instance of ServerMetadataCache.
     * @param env RegionCoprocessorEnvironment
     * @return cache
     */
    public static ServerMetadataCache getInstance(RegionCoprocessorEnvironment env) {
        ServerMetadataCache result = INSTANCE;
        if (result == null) {
            synchronized (ServerMetadataCache.class) {
                result = INSTANCE;
                if (result == null) {
                    INSTANCE = result = new ServerMetadataCache(env.getConfiguration());
                }
            }
        }
        return result;
    }

    private ServerMetadataCache(Configuration conf) {
        this.conf = conf;
        long maxTTL = conf.getLong(
                PHOENIX_COPROC_REGIONSERVER_CACHE_TTL_MS,
                DEFAULT_PHOENIX_COPROC_REGIONSERVER_CACHE_TTL_MS);
        lastDDLTimestampMap = CacheBuilder.newBuilder()
                .removalListener((RemovalListener<ImmutableBytesPtr, Long>) notification -> {
                    String key = notification.getKey().toString();
                    LOGGER.debug("Expiring " + key + " because of "
                            + notification.getCause().name());
                })
                // maximum number of entries this cache can handle.
                .maximumSize(10000L)
                .expireAfterAccess(maxTTL, TimeUnit.MILLISECONDS)
                .build();
    }

    /**
     * Returns the last DDL timestamp from the table. If not found in cache, then query SYSCAT regionserver.
     * @param maintainer
     * @return last DDL timestamp
     * @throws Exception
     */
    // TODO Need to think how to handle SQLException?
    public long getLastDDLTimestampForTable(DDLTimestampMaintainersProtos.DDLTimestampMaintainer maintainer)
            throws IOException {
        byte[] tenantID = maintainer.getTenantID().toByteArray();
        byte[] schemaName = maintainer.getSchemaName().toByteArray();
        byte[] tableName = maintainer.getTableName().toByteArray();
        String fullTableNameStr = SchemaUtil.getTableName(schemaName, tableName);
        byte[] tableKey = SchemaUtil.getTableKey(tenantID, schemaName, tableName);
        ImmutableBytesPtr tableKeyPtr = new ImmutableBytesPtr(tableKey);
        // Lookup in cache if present.
        Long lastDDLTimestamp = lastDDLTimestampMap.getIfPresent(tableKeyPtr);
        if (lastDDLTimestamp != null) {
            LOGGER.trace("Retrieving last ddl timestamp value from cache for tableName: {}",
                    fullTableNameStr);
            return lastDDLTimestamp;
        }

        PTable table;
        String tenantIDStr = Bytes.toString(tenantID);
        if (tenantIDStr == null || tenantIDStr.isEmpty()) {
            tenantIDStr = null;
        }
        Properties properties = new Properties();
        if (tenantIDStr != null) {
            properties.setProperty(TENANT_ID_ATTRIB, tenantIDStr);
        }
        try (Connection connection = QueryUtil.getConnectionOnServer(properties, this.conf)) {
            // Using PhoenixRuntime#getTableNoCache since se don't want to read cached value.
            table = PhoenixRuntime.getTableNoCache(connection, fullTableNameStr);
            // Update cache with the latest DDL timestamp from SYSCAT server.
            lastDDLTimestampMap.put(tableKeyPtr, table.getLastDDLTimestamp());
        } catch (SQLException sqle) {
            // TODO Think what exception to throw in this case?
            LOGGER.warn("Exception while calling calling getTableNoCache for tenant id: {}," +
                            " tableName: {}", tenantIDStr, fullTableNameStr, sqle);
            throw new IOException(sqle);
        }
        return table.getLastDDLTimestamp();
    }

    public static  void resetCache() {
        LOGGER.info("Resetting ServerMetadataCache");
        INSTANCE = null;
    }
}

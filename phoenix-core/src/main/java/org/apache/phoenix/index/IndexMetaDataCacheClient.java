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
package org.apache.phoenix.index;

import static org.apache.phoenix.query.QueryServices.INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.cache.ServerCacheClient;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.join.MaxServerCacheSizeExceededException;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ScanUtil;

public class IndexMetaDataCacheClient {

    private final ServerCacheClient serverCache;
    private TableRef cacheUsingTableRef;
    
    /**
     * Construct client used to send index metadata to each region server
     * for caching during batched put for secondary index maintenance.
     * @param connection the client connection
     * @param cacheUsingTableRef table ref to table that will use the cache during its scan
     */
    public IndexMetaDataCacheClient(PhoenixConnection connection, TableRef cacheUsingTableRef) {
        serverCache = new ServerCacheClient(connection);
        this.cacheUsingTableRef = cacheUsingTableRef;
    }

    /**
     * Determines whether or not to use the IndexMetaDataCache to send the index metadata
     * to the region servers. The alternative is to just set the index metadata as an attribute on
     * the mutations.
     * @param connection 
     * @param mutations the list of mutations that will be sent in a batch to server
     * @param indexMetaDataByteLength length in bytes of the index metadata cache
     */
    public static boolean useIndexMetadataCache(PhoenixConnection connection, List<Mutation> mutations, int indexMetaDataByteLength) {
        ReadOnlyProps props = connection.getQueryServices().getProps();
        int threshold = props.getInt(INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB, QueryServicesOptions.DEFAULT_INDEX_MUTATE_BATCH_SIZE_THRESHOLD);
        return (indexMetaDataByteLength > ServerCacheClient.UUID_LENGTH && mutations.size() > threshold);
    }
    
    /**
     * Send the index metadata cahce to all region servers for regions that will handle the mutations.
     * @return client-side {@link ServerCache} representing the added index metadata cache
     * @throws SQLException 
     * @throws MaxServerCacheSizeExceededException if size of hash cache exceeds max allowed
     * size
     */
    public ServerCache addIndexMetadataCache(List<Mutation> mutations, ImmutableBytesWritable ptr) throws SQLException {
        /**
         * Serialize and compress hashCacheTable
         */
        return serverCache.addServerCache(ScanUtil.newScanRanges(mutations), ptr, new IndexMetaDataCacheFactory(), cacheUsingTableRef);
    }
    
    
    /**
     * Send the index metadata cahce to all region servers for regions that will handle the mutations.
     * @return client-side {@link ServerCache} representing the added index metadata cache
     * @throws SQLException 
     * @throws MaxServerCacheSizeExceededException if size of hash cache exceeds max allowed
     * size
     */
    public ServerCache addIndexMetadataCache(ScanRanges ranges, ImmutableBytesWritable ptr) throws SQLException {
        /**
         * Serialize and compress hashCacheTable
         */
        return serverCache.addServerCache(ranges, ptr, new IndexMetaDataCacheFactory(), cacheUsingTableRef);
    }
}

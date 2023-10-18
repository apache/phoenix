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
package org.apache.phoenix.compat.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CompatUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        CompatUtil.class);

    private static boolean hasFixedShortCircuitConnection =
            VersionInfo.compareVersion(VersionInfo.getVersion(), "2.4.12") >= 0;

    private CompatUtil() {
        //Not to be instantiated
    }

    public static int getCellSerializedSize(Cell cell) {
        return cell.getSerializedSize();
    }

    public static ListMultimap<String, ? extends Permission> readPermissions(
            byte[] data, Configuration conf) throws DeserializationException {
        return PermissionStorage.readPermissions(data, conf);
    }

    public static HFileContext createHFileContext(Configuration conf, Algorithm compression,
            Integer blockSize, DataBlockEncoding encoding, CellComparator comparator) {

        return new HFileContextBuilder()
            .withCompression(compression)
            .withChecksumType(StoreUtils.getChecksumType(conf))
            .withBytesPerCheckSum(StoreUtils.getBytesPerChecksum(conf))
            .withBlockSize(blockSize)
            .withDataBlockEncoding(encoding)
            .build();
    }

    public static HFileContextBuilder withComparator(HFileContextBuilder contextBuilder,
            CellComparatorImpl cellComparator) {
        return contextBuilder.withCellComparator(cellComparator);
    }

    public static StoreFileWriter.Builder withComparator(StoreFileWriter.Builder builder,
            CellComparatorImpl cellComparator) {
        return builder;
    }

    public static Scan getScanForTableName(Connection conn, TableName tableName) {
        return MetaTableAccessor.getScanForTableName(conn.getConfiguration(), tableName);
    }


    /**
     * HBase 2.3+ has storeRefCount available in RegionMetrics
     *
     * @param admin Admin instance
     * @return true if any region has refCount leakage
     * @throws IOException if something went wrong while connecting to Admin
     */
    public synchronized static boolean isAnyStoreRefCountLeaked(Admin admin)
            throws IOException {
        int retries = 5;
        while (retries > 0) {
            boolean isStoreRefCountLeaked = isStoreRefCountLeaked(admin);
            if (!isStoreRefCountLeaked) {
                return false;
            }
            retries--;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted while sleeping", e);
                break;
            }
        }
        return true;
    }

    private static boolean isStoreRefCountLeaked(Admin admin)
            throws IOException {
        for (ServerName serverName : admin.getRegionServers()) {
            for (RegionMetrics regionMetrics : admin.getRegionMetrics(serverName)) {
                if (regionMetrics.getNameAsString().
                    contains(TableName.META_TABLE_NAME.getNameAsString())) {
                    // Just because something is trying to read from hbase:meta in the background
                    // doesn't mean we leaked a scanner, so skip this
                    continue;
                }
                int regionTotalRefCount = regionMetrics.getStoreRefCount();
                if (regionTotalRefCount > 0) {
                    LOGGER.error("Region {} has refCount leak. Total refCount"
                            + " of all storeFiles combined for the region: {}",
                        regionMetrics.getNameAsString(), regionTotalRefCount);
                    return true;
                }
            }
        }
        return false;
    }

    public static ChecksumType getChecksumType(Configuration conf) {
        return StoreUtils.getChecksumType(conf);
    }

    public static int getBytesPerChecksum(Configuration conf) {
        return StoreUtils.getBytesPerChecksum(conf);
    }

    public static Connection createShortCircuitConnection(final Configuration configuration,
            final RegionCoprocessorEnvironment env) throws IOException {
        if (hasFixedShortCircuitConnection) {
            return env.createConnection(configuration);
        } else {
            return org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(configuration);
        }
    }

    public static List<RegionInfo> getMergeRegions(Connection conn, RegionInfo regionInfo)
            throws IOException {
        return MetaTableAccessor.getMergeRegions(conn, regionInfo.getRegionName());
    }

    /**
     * Initialize region for snapshot scanner utility. This is client side region initialization and
     * hence it should follow the same region init pattern as the one used by hbase
     * ClientSideRegionScanner.
     *
     * @param conf The configuration.
     * @param fs The filesystem instance.
     * @param rootDir Restored region root dir.
     * @param htd The table descriptor instance used to retrieve the region root dir.
     * @param hri The region info.
     * @throws IOException If region init throws IOException.
     */
    public static HRegion initRegionForSnapshotScanner(Configuration conf, FileSystem fs,
            Path rootDir, TableDescriptor htd, RegionInfo hri) throws IOException {
        HRegion region =
                HRegion.newHRegion(CommonFSUtils.getTableDir(rootDir, htd.getTableName()), null, fs,
                    conf, hri, htd, null);
        region.setRestoredRegion(true);
        // non RS process does not have a block cache, and this a client side scanner,
        // create one for MapReduce jobs to cache the INDEX block by setting to use
        // IndexOnlyLruBlockCache and set a value to HBASE_CLIENT_SCANNER_BLOCK_CACHE_SIZE_KEY
        conf.set(BlockCacheFactory.BLOCKCACHE_POLICY_KEY, "IndexOnlyLRU");
        // HConstants.HFILE_ONHEAP_BLOCK_CACHE_FIXED_SIZE_KEY is only available from 2.4.6+
        // We are using the string directly here to let Phoenix compile with earlier version,
        // Note that it won't do anything before HBase 2.4.6
        conf.setIfUnset("hfile.onheap.block.cache.fixed.size", String.valueOf(32 * 1024 * 1024L));
        // don't allow L2 bucket cache for non RS process to avoid unexpected disk usage.
        conf.unset(HConstants.BUCKET_CACHE_IOENGINE_KEY);
        region.setBlockCache(BlockCacheFactory.createBlockCache(conf));
        // we won't initialize the MobFileCache when not running in RS process. so provided an
        // initialized cache. Consider the case: an CF was set from an mob to non-mob. if we only
        // initialize cache for MOB region, NPE from HMobStore will still happen. So Initialize the
        // cache for every region although it may hasn't any mob CF, BTW the cache is very
        // light-weight.
        region.setMobFileCache(new MobFileCache(conf));
        region.initialize();
        return region;
    }
}

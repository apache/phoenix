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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.NavigableSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.client.Scan;

public class ScanInfoUtil {
    public static final String PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY =
        "phoenix.max.lookback.age.seconds";
    public static final int DEFAULT_PHOENIX_MAX_LOOKBACK_AGE = 0;

    private ScanInfoUtil() {
    }

    public static boolean isKeepDeletedCells(ScanInfo scanInfo) {
        return scanInfo.getKeepDeletedCells() != KeepDeletedCells.FALSE;
    }

    public static ScanInfo cloneScanInfoWithKeepDeletedCells(ScanInfo scanInfo) {
        return new ScanInfo(scanInfo.getConfiguration(), scanInfo.getFamily(), scanInfo.getMinVersions(),
                    scanInfo.getMaxVersions(), scanInfo.getTtl(), KeepDeletedCells.TRUE,
                    scanInfo.getTimeToPurgeDeletes(), scanInfo.getComparator());
    }

    public static StoreScanner createStoreScanner(Store store, ScanInfo scanInfo, Scan scan,
                                                  final NavigableSet<byte[]> columns,long readPt)
        throws IOException {
        if(!scan.isReversed()) {
            return new StoreScanner(store, scanInfo, scan, columns,readPt);
        } else {
            return new ReversedStoreScanner(store, scanInfo, scan, columns,readPt);
        }
    }

    public static long getTimeToLiveForCompactions(HColumnDescriptor columnDescriptor,
                                                   ScanInfo scanInfo) {
        long ttl = scanInfo.getTtl();
        long maxLookbackTtl = getMaxLookbackInMillis(scanInfo.getConfiguration());
        if (isMaxLookbackTimeEnabled(maxLookbackTtl)) {
            if (ttl == Long.MAX_VALUE
                && columnDescriptor.getKeepDeletedCells() != KeepDeletedCells.TRUE) {
                // If user configured default TTL(FOREVER) and keep deleted cells to false or
                // TTL then to remove unwanted delete markers we should change ttl to max lookback age
                ttl = maxLookbackTtl;
            } else {
                //if there is a TTL, use TTL instead of max lookback age.
                // Max lookback age should be more recent or equal to TTL
                ttl = Math.max(ttl, maxLookbackTtl);
            }
        }

        return ttl;
    }

    /*
     * If KeepDeletedCells.FALSE, KeepDeletedCells.TTL ,
     * let delete markers age once lookback age is done.
     */
    private static KeepDeletedCells getKeepDeletedCells(final Store store, ScanType scanType) {
        //if we're doing a minor compaction or flush, always set keep deleted cells
        //to true. Otherwise, if keep deleted cells is false or TTL, use KeepDeletedCells TTL,
        //where the value of the ttl might be overriden to the max lookback age elsewhere
        return (store.getFamily().getKeepDeletedCells() == KeepDeletedCells.TRUE
            || scanType.equals(ScanType.COMPACT_RETAIN_DELETES)) ?
            KeepDeletedCells.TRUE : KeepDeletedCells.TTL;
    }

    /*
     * if the user set a TTL we should leave MIN_VERSIONS at the default (0 in most of the cases).
     * Otherwise the data (1st version) will not be removed after the TTL. If no TTL, we want
     * Math.max(maxVersions, minVersions, 1)
     */
    private static int getMinVersions(ScanInfo oldScanInfo, final Store store) {
        return oldScanInfo.getTtl() != Long.MAX_VALUE ? store.getFamily().getMinVersions()
            : Math.max(Math.max(store.getFamily().getMinVersions(),
            store.getFamily().getMaxVersions()),1);
    }

    public static ScanInfo getScanInfoForFlushesAndCompactions(Configuration conf,
                                                               ScanInfo oldScanInfo,
                                                         final Store store,
                                                         ScanType type) {
        long ttl = getTimeToLiveForCompactions(store.getFamily(), oldScanInfo);
        KeepDeletedCells keepDeletedCells = getKeepDeletedCells(store, type);
        int minVersions = getMinVersions(oldScanInfo, store);
        return new ScanInfo(conf,store.getFamily().getName(), minVersions,
            Integer.MAX_VALUE, ttl, keepDeletedCells,
            oldScanInfo.getTimeToPurgeDeletes(),
            oldScanInfo.getComparator());
    }

    public static long getMaxLookbackInMillis(Configuration conf){
        //config param is in seconds, switch to millis
        return conf.getLong(PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
            DEFAULT_PHOENIX_MAX_LOOKBACK_AGE) * 1000;
    }

    public static boolean isMaxLookbackTimeEnabled(Configuration conf){
        return isMaxLookbackTimeEnabled(conf.getLong(PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
            DEFAULT_PHOENIX_MAX_LOOKBACK_AGE));
    }

    public static boolean isMaxLookbackTimeEnabled(long maxLookbackTime){
        return maxLookbackTime > 0L;
    }
}

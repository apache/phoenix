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

import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.metrics.Gauge;
import org.apache.hadoop.hbase.metrics.impl.MetricRegistriesImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CompatUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        CompatUtil.class);

    public static Class[] getMrMetricsClasses() {
        return new Class[] { Gauge.class, MetricRegistriesImpl.class };
    }

    public static Scan setStartRow(Scan scan, byte[] indexRowKey, boolean inclusive) {
        return scan.withStartRow(indexRowKey, inclusive);
    }

    public static Scan setSingleRow(Scan scan, byte[] indexRowKey) {
        return scan.withStartRow(indexRowKey, true).withStopRow(indexRowKey, true);
    }

    /**
     * HBase 1.5+ has storeRefCount available in RegionMetrics
     *
     * @param master Active HMaster instance
     * @return true if any region has refCount leakage
     */
    public synchronized static boolean isAnyStoreRefCountLeaked(HMaster master) {
        int retries = 5;
        while (retries > 0) {
            boolean isStoreRefCountLeaked = isStoreRefCountLeaked(master);
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

    private static boolean isStoreRefCountLeaked(HMaster master) {
        for (ServerLoad serverLoad : master.getServerManager().getOnlineServers().values()) {
            for (RegionLoad regionLoad : serverLoad.getRegionsLoad().values()) {
                int regionTotalRefCount = regionLoad.getStoreRefCount();
                if (regionTotalRefCount > 0) {
                    LOGGER.error("Region {} has refCount leak. Total refCount"
                            + " of all storeFiles combined for the region: {}",
                        regionLoad.getNameAsString(), regionTotalRefCount);
                    return true;
                }
            }
        }
        return false;
    }

}

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
package org.apache.phoenix.compat.hbase.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.ScanOptions;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;

import java.io.IOException;

public class CompatBaseScannerRegionObserver implements RegionObserver {

    public static final String PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY =
        "phoenix.max.lookback.age.seconds";
    public static final int DEFAULT_PHOENIX_MAX_LOOKBACK_AGE = 0;

    public void preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                             ScanType scanType, ScanOptions options, CompactionLifeCycleTracker tracker,
                                             CompactionRequest request) throws IOException {
        //no-op because HBASE-24321 isn't present in HBase 2.1.x, so we can't implement the "max
        //lookback age" feature
    }

    public void preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                           ScanOptions options, FlushLifeCycleTracker tracker) throws IOException {
        //no-op because HBASE-24321 isn't present in HBase 2.1.x, so we can't implement the "max
        //lookback age" feature
    }

    public void preMemStoreCompactionCompactScannerOpen(
        ObserverContext<RegionCoprocessorEnvironment> c, Store store, ScanOptions options)
        throws IOException {
        //no-op because HBASE-24321 isn't present in HBase 2.1.x, so we can't implement the "max
        //lookback age" feature
    }

    public void preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Store store,
                                           ScanOptions options) throws IOException {
        //no-op because HBASE-24321 isn't present in HBase 2.1.x, so we can't override the scan
        //to "look behind" delete markers on SCN queries
    }

    public static long getMaxLookbackInMillis(Configuration conf){
        //config param is in seconds, switch to millis
        return conf.getLong(PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
            DEFAULT_PHOENIX_MAX_LOOKBACK_AGE) * 1000;
    }

    //max lookback age isn't supported in HBase 2.1 or HBase 2.2
    public static boolean isMaxLookbackTimeEnabled(Configuration conf){
        return false;
    }

    public static boolean isMaxLookbackTimeEnabled(long maxLookbackTime){
        return false;
    }

}

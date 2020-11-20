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
package org.apache.phoenix.coprocessor;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.filter.SyscatViewIndexIdFilter;
import org.apache.phoenix.util.ScanUtil;

import java.io.IOException;

import static org.apache.phoenix.util.ScanUtil.UNKNOWN_CLIENT_VERSION;

/**
 * Coprocessor that checks whether the VIEW_INDEX_ID needs to retrieve.
 */
public class SyscatRegionObserver extends BaseRegionObserver {
    @Override public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
    }

    @Override public void stop(CoprocessorEnvironment e) throws IOException {
        super.stop(e);
    }

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,
                                        RegionScanner s) throws IOException {
        int clientVersion = ScanUtil.getClientVersion(scan);
        if (clientVersion != UNKNOWN_CLIENT_VERSION) {
            ScanUtil.andFilterAtBeginning(scan, new SyscatViewIndexIdFilter(clientVersion));
        }
        return s;
    }
}
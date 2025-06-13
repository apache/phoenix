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

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Test implementation of PhoenixMasterObserver that simulates failures for a specific number
 * of retries before succeeding.
 */
public class TestPhoenixMasterObserver extends PhoenixMasterObserver {

    private int splitFailureCount;
    private int mergeFailureCount;

    private static final int SPLIT_FAILURE_THRESHOLD = 24;
    private static final int MERGE_FAILURE_THRESHOLD = 15;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        super.start(env);
        splitFailureCount = 0;
        mergeFailureCount = 0;
    }

    @Override
    protected List<String> getAncestorIdsForSplit(Connection conn, String tableName,
                                                  String streamName, RegionInfo regionInfoA,
                                                  RegionInfo regionInfoB) throws SQLException {
        if (splitFailureCount < SPLIT_FAILURE_THRESHOLD) {
            splitFailureCount++;
            throw new SQLException(
                    "Test failure for split operation, attempt " + splitFailureCount);
        }
        return super.getAncestorIdsForSplit(conn, tableName, streamName, regionInfoA, regionInfoB);
    }

    @Override
    protected List<String> getAncestorIdsForMerge(Connection conn, String tableName,
                                                  String streamName, RegionInfo parent)
            throws SQLException {
        if (mergeFailureCount < MERGE_FAILURE_THRESHOLD) {
            mergeFailureCount++;
            throw new SQLException(
                    "Test failure for merge operation, attempt " + mergeFailureCount);
        }
        return super.getAncestorIdsForMerge(conn, tableName, streamName, parent);
    }

}
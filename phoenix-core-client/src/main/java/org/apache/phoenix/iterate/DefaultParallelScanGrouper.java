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
package org.apache.phoenix.iterate;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.util.ScanUtil;

/**
 * Default implementation that creates a scan group if a plan is row key ordered (which requires a merge sort),
 * or if a scan crosses a region boundary and the table is salted or a local index.
 */
public class DefaultParallelScanGrouper implements ParallelScanGrouper {

    private static DefaultParallelScanGrouper INSTANCE = new DefaultParallelScanGrouper();

    public DefaultParallelScanGrouper() {
    }

    public static DefaultParallelScanGrouper getInstance() {
        return INSTANCE;
    }

    /**
     * Returns true if the scan with the startKey is to be the first of a new batch
     */
    @Override
    public boolean shouldStartNewScan(QueryPlan plan, Scan lastScan, byte[] startKey,
            boolean crossesRegionBoundary) {
        PTable table = plan.getTableRef().getTable();
        if (lastScan == null) {
            return false;
        } else if (!plan.isRowKeyOrdered()) {
            return true;
        } else if (crossesRegionBoundary && table.getIndexType() == IndexType.LOCAL) {
            return true;
        } else if (table.getBucketNum() != null ) {
            return crossesRegionBoundary
                    || ScanUtil.crossesPrefixBoundary(startKey,
                        ScanUtil.getPrefix(lastScan.getStartRow(),
                            SaltingUtil.NUM_SALTING_BYTES),
                        SaltingUtil.NUM_SALTING_BYTES);
        } else {
            return false;
        }
    }

    @Override
    public List<HRegionLocation> getRegionBoundaries(StatementContext context, byte[] tableName)
            throws SQLException {
        return context.getConnection().getQueryServices().getAllTableRegions(tableName,
                context.getStatement().getQueryTimeoutInMillis());
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<HRegionLocation> getRegionBoundaries(StatementContext context,
        byte[] tableName, byte[] startRegionBoundaryKey, byte[] stopRegionBoundaryKey)
        throws SQLException {
        return context.getConnection().getQueryServices()
            .getTableRegions(tableName, startRegionBoundaryKey, stopRegionBoundaryKey,
                    context.getStatement().getQueryTimeoutInMillis());
    }
}

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

/**
 * Interface for a parallel scan grouper
 */
public interface ParallelScanGrouper {

    /**
     * Determines whether to create a new group of parallel scans.
     * @param plan current query plan
     * @param lastScan the last scan processed
     * @param startKey of the new scan
     * @param crossesRegionBoundary whether startKey in a different region than lastScan
     * @return true if we should create a new group of scans, starting with the scan whose start
     *         key we passed as startKey
     */
	boolean shouldStartNewScan(QueryPlan plan, Scan lastScan, byte[] startKey, boolean crossesRegionBoundary);

	List<HRegionLocation> getRegionBoundaries(StatementContext context, byte[] tableName) throws SQLException;

	/**
	 * Retrieve table region locations that cover the startRegionBoundaryKey and
	 * stopRegionBoundaryKey. The start key of the first region of the returned list must be less
	 * than or equal to startRegionBoundaryKey. The end key of the last region of the returned list
	 * must be greater than or equal to stopRegionBoundaryKey.
	 *
	 * @param context Statement Context.
	 * @param tableName Table name.
	 * @param startRegionBoundaryKey Start region boundary key.
	 * @param stopRegionBoundaryKey Stop region boundary key.
	 * @return The list of region locations that cover the startRegionBoundaryKey and
	 * stopRegionBoundaryKey key boundary.
	 * @throws SQLException If fails to retrieve region locations.
	 */
	List<HRegionLocation> getRegionBoundaries(StatementContext context, byte[] tableName,
			byte[] startRegionBoundaryKey, byte[] stopRegionBoundaryKey) throws SQLException;
}

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

import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;

/**
 * Interface for a parallel scan grouper
 */
public interface ParallelScanGrouper {

	/**
	 * Determines whether to create a new group of parallel scans.
	 * 	
	 * @param scans						current scan group
	 * @param plan						current query plan
	 * @param startKey					start key of scan
	 * @param crossedRegionBoundary		whether we crossed a region boundary
	 * @return true if we should create a new group of scans
	 */
	boolean shouldStartNewScan(QueryPlan plan, List<Scan> scans, byte[] startKey, boolean crossedRegionBoundary);
	
}

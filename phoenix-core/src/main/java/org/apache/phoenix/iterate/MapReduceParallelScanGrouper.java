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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;

/**
 * Scan grouper that creates a scan group if a plan is row key ordered or if a
 * scan crosses region boundaries
 */
public class MapReduceParallelScanGrouper implements ParallelScanGrouper {

	private static final MapReduceParallelScanGrouper INSTANCE = new MapReduceParallelScanGrouper();

  public static MapReduceParallelScanGrouper getInstance() {
		return INSTANCE;
	}

   private MapReduceParallelScanGrouper() {}

	@Override
	public boolean shouldStartNewScan(QueryPlan plan, List<Scan> scans,
			byte[] startKey, boolean crossedRegionBoundary) {
		return !plan.isRowKeyOrdered() || crossedRegionBoundary;
	}

	@Override
	public List<HRegionLocation> getRegionBoundaries(StatementContext context, byte[] tableName) throws SQLException {
		String snapshotName;
		Configuration conf = context.getConnection().getQueryServices().getConfiguration();
		if((snapshotName = getSnapshotName(conf)) != null) {
			try {
				Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
				FileSystem fs = rootDir.getFileSystem(conf);
				Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
				HBaseProtos.SnapshotDescription snapshotDescription = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
				SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDescription);
				return getRegionLocationsFromManifest(manifest);
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		else {
			return context.getConnection().getQueryServices().getAllTableRegions(tableName);
		}
	}

	private List<HRegionLocation> getRegionLocationsFromManifest(SnapshotManifest manifest) {
		List<SnapshotProtos.SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
		Preconditions.checkNotNull(regionManifests);

		List<HRegionLocation> regionLocations = Lists.newArrayListWithCapacity(regionManifests.size());

		for (SnapshotProtos.SnapshotRegionManifest regionManifest : regionManifests) {
			regionLocations.add(new HRegionLocation(
					HRegionInfo.convert(regionManifest.getRegionInfo()), null));
		}

		return regionLocations;
	}

	private String getSnapshotName(Configuration conf) {
		return conf.get(PhoenixConfigurationUtil.SNAPSHOT_NAME_KEY);
	}

}

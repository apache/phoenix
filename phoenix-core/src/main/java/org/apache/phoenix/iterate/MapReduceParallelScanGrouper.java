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

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scan grouper that creates a scan group if a plan is row key ordered or if a
 * scan crosses region boundaries
 */
public class MapReduceParallelScanGrouper implements ParallelScanGrouper {

	private static final MapReduceParallelScanGrouper INSTANCE = new MapReduceParallelScanGrouper();
	private static final Logger LOGGER = LoggerFactory.getLogger(MapReduceParallelScanGrouper.class);

    public static MapReduceParallelScanGrouper getInstance() {
		return INSTANCE;
	}

    @VisibleForTesting
    MapReduceParallelScanGrouper() {}

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
				SnapshotDescription snapshotDescription = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
				//Perform a restore directory cleanup
				cleanup(fs, new Path(conf.get(PhoenixConfigurationUtil.RESTORE_DIR_KEY)));
				//Performing snapshot restore which will be used during scans
				Path restoreDir = new Path(conf.get(PhoenixConfigurationUtil.RESTORE_DIR_KEY));
				RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);
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

	/**
	 * Perform cleanup for snapshot restore directory
	 * @param fs
	 * @param restoreDir
	 */
	private void cleanup(FileSystem fs, Path restoreDir) {
		try {
			if (fs.exists(restoreDir)) {
				if (fs.delete(restoreDir, true)) {
					LOGGER.info("Cleanup performed on snapshot restoreDir: " + restoreDir);
				} else {
					LOGGER.warn("Delete restore directory for the snapshot failed. restoreDir: "
							+ restoreDir);
				}
			}
		} catch (IOException ex) {
			LOGGER.warn("Could not delete restore directory for the snapshot. restoreDir: "
					+ restoreDir, ex);
		}
	}

	/**
	 * Get list of region locations from SnapshotManifest
	 * BaseResultIterators assume that regions are sorted using RegionInfo.COMPARATOR
	 */
	private List<HRegionLocation> getRegionLocationsFromManifest(SnapshotManifest manifest) {
		List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
		Preconditions.checkNotNull(regionManifests);

		List<RegionInfo> regionInfos = Lists.newArrayListWithCapacity(regionManifests.size());
		List<HRegionLocation> hRegionLocations = Lists.newArrayListWithCapacity(regionManifests.size());

		for (SnapshotRegionManifest regionManifest : regionManifests) {
			RegionInfo regionInfo = ProtobufUtil.toRegionInfo(regionManifest.getRegionInfo());
			if (isValidRegion(regionInfo)) {
				regionInfos.add(regionInfo);
			}
		}

		regionInfos.sort(RegionInfo.COMPARATOR);

		for (RegionInfo regionInfo : regionInfos) {
			hRegionLocations.add(new HRegionLocation(regionInfo, null));
		}

		return hRegionLocations;
	}

	// Exclude offline split parent regions
	private boolean isValidRegion(RegionInfo hri) {
		if (hri.isOffline() && (hri.isSplit() || hri.isSplitParent())) {
			return false;
		}
		return true;
	}

	private String getSnapshotName(Configuration conf) {
		return conf.get(PhoenixConfigurationUtil.SNAPSHOT_NAME_KEY);
	}

}

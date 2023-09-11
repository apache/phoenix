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
package org.apache.phoenix.mapreduce;

import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.RegionSizeCalculator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.MRJobType;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.stats.StatisticsUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * {@link InputFormat} implementation from Phoenix.
 * 
 */
public class PhoenixInputFormat<T extends DBWritable> extends InputFormat<NullWritable,T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixInputFormat.class);
       
    /**
     * instantiated by framework
     */
    public PhoenixInputFormat() {
    }

    @Override
    public RecordReader<NullWritable,T> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        final QueryPlan queryPlan = getQueryPlan(context,configuration);
        @SuppressWarnings("unchecked")
        final Class<T> inputClass = (Class<T>) PhoenixConfigurationUtil.getInputClass(configuration);
        return getPhoenixRecordReader(inputClass, configuration, queryPlan);
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {  
        final Configuration configuration = context.getConfiguration();
        final QueryPlan queryPlan = getQueryPlan(context,configuration);
        return generateSplits(queryPlan, configuration);
    }

    /**
     * Randomise the length parameter of the splits to ensure random execution order.
     * Yarn orders splits by size before execution.
     *
     * @param splits
     */
    protected void randomizeSplitLength(List<InputSplit> splits) {
        LOGGER.info("Randomizing split size");
        if (splits.size() == 0) {
            return;
        }
        double defaultLength = 1000000d;
        double totalLength = splits.stream().mapToDouble(s -> {
            try {
                return (double) s.getLength();
            } catch (IOException | InterruptedException e1) {
                return defaultLength;
            }
        }).sum();
        long avgLength = (long) (totalLength / splits.size());
        splits.stream().forEach(s -> ((PhoenixInputSplit) s)
                .setLength(avgLength + ThreadLocalRandom.current().nextInt(10000)));
    }

    protected List<InputSplit> generateSplits(final QueryPlan qplan, Configuration config)
            throws IOException {
        // We must call this in order to initialize the scans and splits from the query plan
        setupParallelScansFromQueryPlan(qplan);
        final List<KeyRange> splits = qplan.getSplits();
        Preconditions.checkNotNull(splits);

        // Get the RegionSizeCalculator
        try (org.apache.hadoop.hbase.client.Connection connection =
                HBaseFactoryProvider.getHConnectionFactory().createConnection(config)) {
            RegionLocator regionLocator =
                    connection.getRegionLocator(TableName
                            .valueOf(qplan.getTableRef().getTable().getPhysicalName().toString()));
            RegionSizeCalculator sizeCalculator =
                    new RegionSizeCalculator(regionLocator, connection.getAdmin());

            final List<InputSplit> psplits = Lists.newArrayListWithExpectedSize(splits.size());
            for (List<Scan> scans : qplan.getScans()) {
                // Get the region location
                HRegionLocation location =
                        regionLocator.getRegionLocation(scans.get(0).getStartRow(), false);

                String regionLocation = location.getHostname();

                // Get the region size
                long regionSize =
                        sizeCalculator.getRegionSize(location.getRegion().getRegionName());

                // Generate splits based off statistics, or just region splits?
                boolean splitByStats = PhoenixConfigurationUtil.getSplitByStats(config);

                if (splitByStats) {
                    for (Scan aScan : scans) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Split for  scan : " + aScan + "with scanAttribute : "
                                    + aScan.getAttributesMap()
                                    + " [scanCache, cacheBlock, scanBatch] : [" + aScan.getCaching()
                                    + ", " + aScan.getCacheBlocks() + ", " + aScan.getBatch()
                                    + "] and  regionLocation : " + regionLocation);
                        }

                        // The size is bogus, but it's not a problem
                        psplits.add(new PhoenixInputSplit(Collections.singletonList(aScan),
                                regionSize, regionLocation));
                    }
                } else {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Scan count[" + scans.size() + "] : "
                                + Bytes.toStringBinary(scans.get(0).getStartRow()) + " ~ "
                                + Bytes.toStringBinary(scans.get(scans.size() - 1).getStopRow()));
                        LOGGER.debug("First scan : " + scans.get(0) + "with scanAttribute : "
                                + scans.get(0).getAttributesMap()
                                + " [scanCache, cacheBlock, scanBatch] : " + "["
                                + scans.get(0).getCaching() + ", " + scans.get(0).getCacheBlocks()
                                + ", " + scans.get(0).getBatch() + "] and  regionLocation : "
                                + regionLocation);

                        for (int i = 0, limit = scans.size(); i < limit; i++) {
                            LOGGER.debug("EXPECTED_UPPER_REGION_KEY[" + i + "] : "
                                    + Bytes.toStringBinary(scans.get(i).getAttribute(
                                        BaseScannerRegionObserverConstants.EXPECTED_UPPER_REGION_KEY)));
                        }
                    }

                    psplits.add(new PhoenixInputSplit(scans, regionSize, regionLocation));
                }
            }

            if (PhoenixConfigurationUtil.isMRRandomizeMapperExecutionOrder(config)) {
                randomizeSplitLength(psplits);
            }

            return psplits;
        }
    }

    /**
     * Returns the query plan associated with the select query.
     * @param context
     * @return
     * @throws IOException
     */
    protected  QueryPlan getQueryPlan(final JobContext context, final Configuration configuration)
            throws IOException {
        Preconditions.checkNotNull(context);
        try {
            final String txnScnValue = configuration.get(PhoenixConfigurationUtil.TX_SCN_VALUE);
            final String currentScnValue = configuration.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
            final String tenantId = configuration.get(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID);
            final Properties overridingProps = new Properties();
            if (txnScnValue == null && currentScnValue != null) {
                overridingProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, currentScnValue);
            }
            if (tenantId != null && configuration.get(PhoenixRuntime.TENANT_ID_ATTRIB) == null){
                overridingProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            }
            try (final Connection connection = ConnectionUtil.getInputConnection(configuration, overridingProps);
                 final Statement statement = connection.createStatement()) {

              MRJobType mrJobType = PhoenixConfigurationUtil.getMRJobType(configuration, MRJobType.QUERY.name());

              String selectStatement;
              switch (mrJobType) {
                  case UPDATE_STATS:
                      // This select statement indicates MR job for full table scan for stats collection
                      selectStatement = "SELECT * FROM " + PhoenixConfigurationUtil.getInputTableName(configuration);
                      break;
                  default:
                      selectStatement = PhoenixConfigurationUtil.getSelectStatement(configuration);
              }
              Preconditions.checkNotNull(selectStatement);

              final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
              // Optimize the query plan so that we potentially use secondary indexes
              final QueryPlan queryPlan = pstmt.optimizeQuery(selectStatement);
              final Scan scan = queryPlan.getContext().getScan();

              if (mrJobType == MRJobType.UPDATE_STATS) {
                  StatisticsUtil.setScanAttributes(scan, null);
              }

              // since we can't set a scn on connections with txn set TX_SCN attribute so that the max time range is set by BaseScannerRegionObserver
              if (txnScnValue != null) {
                scan.setAttribute(BaseScannerRegionObserverConstants.TX_SCN, Bytes.toBytes(Long.parseLong(txnScnValue)));
              }

              // setting the snapshot configuration
              String snapshotName = configuration.get(PhoenixConfigurationUtil.SNAPSHOT_NAME_KEY);
              String restoreDir = configuration.get(PhoenixConfigurationUtil.RESTORE_DIR_KEY);
              boolean isSnapshotRestoreManagedExternally = PhoenixConfigurationUtil.isMRSnapshotManagedExternally(configuration);
              Configuration config = queryPlan.getContext().
                      getConnection().getQueryServices().getConfiguration();
              if (snapshotName != null) {
                PhoenixConfigurationUtil.setSnapshotNameKey(config, snapshotName);
                PhoenixConfigurationUtil.setRestoreDirKey(config, restoreDir);
                PhoenixConfigurationUtil.setMRSnapshotManagedExternally(config, isSnapshotRestoreManagedExternally);
              } else {
                // making sure we unset snapshot name as new job doesn't need it
                config.unset(PhoenixConfigurationUtil.SNAPSHOT_NAME_KEY);
                config.unset(PhoenixConfigurationUtil.RESTORE_DIR_KEY);
                config.unset(PhoenixConfigurationUtil.MAPREDUCE_EXTERNAL_SNAPSHOT_RESTORE);
              }

              return queryPlan;
            }
        } catch (Exception exception) {
            LOGGER.error(String.format("Failed to get the query plan with error [%s]",
                exception.getMessage()));
            throw new RuntimeException(exception);
        }
    }

    void setupParallelScansFromQueryPlan(QueryPlan queryPlan) {
        setupParallelScansWithScanGrouper(queryPlan, MapReduceParallelScanGrouper.getInstance());
    }

    RecordReader<NullWritable,T> getPhoenixRecordReader(Class<T> inputClass,
            Configuration configuration, QueryPlan queryPlan) {
        return new PhoenixRecordReader<>(inputClass , configuration, queryPlan,
                MapReduceParallelScanGrouper.getInstance());
    }

    /**
     * Initialize the query plan so it sets up the parallel scans
     * @param queryPlan Query plan corresponding to the select query
     * @param scanGrouper Parallel scan grouper
     */
    void setupParallelScansWithScanGrouper(QueryPlan queryPlan, ParallelScanGrouper scanGrouper) {
        Preconditions.checkNotNull(queryPlan);
        try {
            queryPlan.iterator(scanGrouper);
        } catch (SQLException e) {
            LOGGER.error(String.format("Setting up parallel scans for the query plan failed "
                    + "with error [%s]", e.getMessage()));
            throw new RuntimeException(e);
        }
    }

}

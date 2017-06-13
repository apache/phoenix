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
package org.apache.phoenix.mapreduce.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.iterate.*;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.MultiTableInputSplit;
import org.apache.phoenix.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.SchemaType;
import org.apache.phoenix.monitoring.ReadMetricQueue;
import org.apache.phoenix.monitoring.ScanMetricsHolder;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.PhoenixRuntime;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.UUID;

/**
 * Utility class for setting Configuration parameters for the Map Reduce job
 */
public final class PhoenixMapReduceUtil {

    private static final Log LOG = LogFactory.getLog(PhoenixMapReduceUtil.class);

    private PhoenixMapReduceUtil() {

    }

    /**
     *
     * @param job
     * @param inputClass DBWritable class
     * @param tableName  Input table name
     * @param conditions Condition clause to be added to the WHERE clause. Can be <tt>null</tt> if there are no conditions.
     * @param fieldNames fields being projected for the SELECT query.
     */
    public static void setInput(final Job job, final Class<? extends DBWritable> inputClass, final String tableName,
                                final String conditions, final String... fieldNames) {
        final Configuration configuration = setInput(job, inputClass, tableName);
        if(conditions != null) {
            PhoenixConfigurationUtil.setInputTableConditions(configuration, conditions);
        }
        PhoenixConfigurationUtil.setSelectColumnNames(configuration, fieldNames);
    }

    /**
     *
     * @param job
     * @param inputClass  DBWritable class
     * @param tableName   Input table name
     * @param inputQuery  Select query.
     */
    public static void setInput(final Job job, final Class<? extends DBWritable> inputClass, final String tableName, final String inputQuery) {
        final Configuration configuration = setInput(job, inputClass, tableName);
        PhoenixConfigurationUtil.setInputQuery(configuration, inputQuery);
        PhoenixConfigurationUtil.setSchemaType(configuration, SchemaType.QUERY);
     }

    /**
     *
     * @param job
     * @param inputClass DBWritable class
     * @param snapshotName The name of a snapshot (of a table) to read from
     * @param tableName Input table name
     * @param restoreDir a temporary dir to copy the snapshot files into
     * @param conditions Condition clause to be added to the WHERE clause. Can be <tt>null</tt> if there are no conditions.
     * @param fieldNames fields being projected for the SELECT query.
     */
    public static void setInput(final Job job, final Class<? extends DBWritable> inputClass, final String snapshotName, String tableName,
        Path restoreDir, final String conditions, final String... fieldNames) throws
        IOException {
        final Configuration configuration = setSnapshotInput(job, inputClass, snapshotName, tableName, restoreDir);
        if(conditions != null) {
            PhoenixConfigurationUtil.setInputTableConditions(configuration, conditions);
        }
        PhoenixConfigurationUtil.setSelectColumnNames(configuration, fieldNames);
    }

    /**
     *
     * @param job
     * @param inputClass DBWritable class
     * @param snapshotName The name of a snapshot (of a table) to read from
     * @param tableName Input table name
     * @param restoreDir a temporary dir to copy the snapshot files into
     * @param inputQuery The select query
     */
    public static void setInput(final Job job, final Class<? extends DBWritable> inputClass, final String snapshotName, String tableName,
        Path restoreDir, String inputQuery) throws
        IOException {
        final Configuration configuration = setSnapshotInput(job, inputClass, snapshotName, tableName, restoreDir);
        if(inputQuery != null) {
            PhoenixConfigurationUtil.setInputQuery(configuration, inputQuery);
        }

    }

    /**
     *
     * @param job
     * @param inputClass DBWritable class
     * @param snapshotName The name of a snapshot (of a table) to read from
     * @param tableName Input table name
     * @param restoreDir a temporary dir to copy the snapshot files into
     */
    private static Configuration setSnapshotInput(Job job, Class<? extends DBWritable> inputClass, String snapshotName,
        String tableName, Path restoreDir) {
        job.setInputFormatClass(PhoenixInputFormat.class);
        final Configuration configuration = job.getConfiguration();
        PhoenixConfigurationUtil.setInputClass(configuration, inputClass);
        PhoenixConfigurationUtil.setSnapshotNameKey(configuration, snapshotName);
        PhoenixConfigurationUtil.setInputTableName(configuration, tableName);

        PhoenixConfigurationUtil.setRestoreDirKey(configuration, new Path(restoreDir, UUID.randomUUID().toString()).toString());
        PhoenixConfigurationUtil.setSchemaType(configuration, SchemaType.QUERY);
        return configuration;
    }

    private static Configuration setInput(final Job job, final Class<? extends DBWritable> inputClass, final String tableName){
        job.setInputFormatClass(PhoenixInputFormat.class);
        final Configuration configuration = job.getConfiguration();
        PhoenixConfigurationUtil.setInputTableName(configuration, tableName);
        PhoenixConfigurationUtil.setInputClass(configuration,inputClass);
        return configuration;
    }

    /**
     * A method to override which HBase cluster for {@link PhoenixInputFormat} to read from
     * @param job MapReduce Job
     * @param quorum an HBase cluster's ZooKeeper quorum
     */
    public static void setInputCluster(final Job job, final String quorum) {
        final Configuration configuration = job.getConfiguration();
        PhoenixConfigurationUtil.setInputCluster(configuration, quorum);
    }
    /**
     *
     * @param job
     * @param outputClass
     * @param tableName  Output table
     * @param columns    List of columns separated by ,
     */
    public static void setOutput(final Job job, final String tableName,final String columns) {
        job.setOutputFormatClass(PhoenixOutputFormat.class);
        final Configuration configuration = job.getConfiguration();
        PhoenixConfigurationUtil.setOutputTableName(configuration, tableName);
        PhoenixConfigurationUtil.setUpsertColumnNames(configuration,columns.split(","));
    }


    /**
     *
     * @param job
     * @param outputClass
     * @param tableName  Output table
     * @param fieldNames fields
     */
    public static void setOutput(final Job job, final String tableName , final String... fieldNames) {
          job.setOutputFormatClass(PhoenixOutputFormat.class);
          final Configuration configuration = job.getConfiguration();
          PhoenixConfigurationUtil.setOutputTableName(configuration, tableName);
          PhoenixConfigurationUtil.setUpsertColumnNames(configuration,fieldNames);
    }

    /**
     * A method to override which HBase cluster for {@link PhoenixOutputFormat} to write to
     * @param job MapReduce Job
     * @param quorum an HBase cluster's ZooKeeper quorum
     */
    public static void setOutputCluster(final Job job, final String quorum) {
        final Configuration configuration = job.getConfiguration();
        PhoenixConfigurationUtil.setOutputCluster(configuration, quorum);
    }

    /**
     * Generate a query plan for a MapReduce job query.
     * @param configuration The MapReduce job configuration
     * @return Query plan for the MapReduce job
     * @throws SQLException If the plan cannot be generated
     */
    public static QueryPlan getQueryPlan(final Configuration configuration)
            throws SQLException {
        return getQueryPlan(configuration, false);
    }

    /**
     * Generate a query plan for a MapReduce job query
     * @param configuration The MapReduce job configuration
     * @param isTargetConnection Whether the query plan is for the target HBase cluster
     * @return Query plan for the MapReduce job
     * @throws SQLException If the plan cannot be generated
     */
    public static QueryPlan getQueryPlan(final Configuration configuration,
            boolean isTargetConnection) throws SQLException {
        Preconditions.checkNotNull(configuration);
        final String txnScnValue = configuration.get(PhoenixConfigurationUtil.TX_SCN_VALUE);
        final String currentScnValue = configuration.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
        final Properties overridingProps = new Properties();
        if(txnScnValue==null && currentScnValue!=null) {
            overridingProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, currentScnValue);
        }
        final Connection connection;
        final String selectStatement;
        if (isTargetConnection) {
            String targetTable = PhoenixConfigurationUtil.getInputTargetTableName(configuration);
            if (!Strings.isNullOrEmpty(targetTable)) {
                // different table on same cluster
                connection = ConnectionUtil.getInputConnection(configuration, overridingProps);
                selectStatement = PhoenixConfigurationUtil.getSelectStatement(configuration, true);
            } else {
                // same table on different cluster
                connection =
                        ConnectionUtil.getTargetInputConnection(configuration, overridingProps);
                selectStatement = PhoenixConfigurationUtil.getSelectStatement(configuration);
            }
        } else {
            connection = ConnectionUtil.getInputConnection(configuration, overridingProps);
            selectStatement = PhoenixConfigurationUtil.getSelectStatement(configuration);
        }
        Preconditions.checkNotNull(selectStatement);
        final Statement statement = connection.createStatement();
        final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
        // Optimize the query plan so that we potentially use secondary indexes
        final QueryPlan queryPlan = pstmt.optimizeQuery(selectStatement);
        final Scan scan = queryPlan.getContext().getScan();
        // since we can't set a scn on connections with txn set TX_SCN attribute so that the max time range is set by BaseScannerRegionObserver
        if (txnScnValue!=null) {
            scan.setAttribute(BaseScannerRegionObserver.TX_SCN, Bytes.toBytes(Long.valueOf(txnScnValue)));
        }
        // Initialize the query plan so it sets up the parallel scans
        queryPlan.iterator(MapReduceParallelScanGrouper.getInstance());
        return queryPlan;
    }

    /**
     * Generates the input splits for a MapReduce job.
     * @param qplan Query plan for the job
     * @param splits The key range splits for the job
     * @param config The job configuration
     * @return Input splits for the job
     * @throws IOException If the region information for the splits cannot be retrieved
     */
    public static List<InputSplit> generateSplits(final QueryPlan qplan,
            final List<KeyRange> splits, Configuration config) throws IOException {
        Preconditions.checkNotNull(qplan);
        Preconditions.checkNotNull(splits);

        // Get the RegionSizeCalculator
        org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(config);
        RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(qplan
                .getTableRef().getTable().getPhysicalName().toString()));
        RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(regionLocator, connection
                .getAdmin());

        final List<InputSplit> psplits = Lists.newArrayListWithExpectedSize(splits.size());
        for (List<Scan> scans : qplan.getScans()) {
            // Get the region location
            HRegionLocation location = regionLocator.getRegionLocation(
                    scans.get(0).getStartRow(),
                    false
            );

            String regionLocation = location.getHostname();

            // Get the region size
            long regionSize = sizeCalculator.getRegionSize(
                    location.getRegionInfo().getRegionName()
            );

            // Generate splits based off statistics, or just region splits?
            boolean splitByStats = PhoenixConfigurationUtil.getSplitByStats(config);

            if(splitByStats) {
                for(Scan aScan: scans) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Split for  scan : " + aScan + "with scanAttribute : " + aScan
                                .getAttributesMap() + " [scanCache, cacheBlock, scanBatch] : [" +
                                aScan.getCaching() + ", " + aScan.getCacheBlocks() + ", " + aScan
                                .getBatch() + "] and  regionLocation : " + regionLocation);
                    }

                    psplits.add(new PhoenixInputSplit(Collections.singletonList(aScan), regionSize, regionLocation));
                }
            }
            else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Scan count[" + scans.size() + "] : " + Bytes.toStringBinary(scans
                            .get(0).getStartRow()) + " ~ " + Bytes.toStringBinary(scans.get(scans
                            .size() - 1).getStopRow()));
                    LOG.debug("First scan : " + scans.get(0) + "with scanAttribute : " + scans
                            .get(0).getAttributesMap() + " [scanCache, cacheBlock, scanBatch] : " +
                            "[" + scans.get(0).getCaching() + ", " + scans.get(0).getCacheBlocks()
                            + ", " + scans.get(0).getBatch() + "] and  regionLocation : " +
                            regionLocation);

                    for (int i = 0, limit = scans.size(); i < limit; i++) {
                        LOG.debug("EXPECTED_UPPER_REGION_KEY[" + i + "] : " + Bytes
                                .toStringBinary(scans.get(i).getAttribute
                                        (BaseScannerRegionObserver.EXPECTED_UPPER_REGION_KEY)));
                    }
                }

                psplits.add(new PhoenixInputSplit(scans, regionSize, regionLocation));
            }
        }
        return psplits;
    }

    public static ResultIterator initializeIterator(Configuration conf, List<Scan> scans,
            QueryPlan queryPlan) throws SQLException, IOException {
        List<PeekingResultIterator> iterators = Lists.newArrayListWithExpectedSize(scans.size());
        StatementContext ctx = queryPlan.getContext();
        ReadMetricQueue readMetrics = ctx.getReadMetricsQueue();
        String tableName = queryPlan.getTableRef().getTable().getPhysicalName().getString();
        String snapshotName = conf.get(PhoenixConfigurationUtil.SNAPSHOT_NAME_KEY);

        // Clear the table region boundary cache to make sure long running jobs stay up to date
        byte[] tableNameBytes = queryPlan.getTableRef().getTable().getPhysicalName().getBytes();
        ConnectionQueryServices services = queryPlan.getContext().getConnection().getQueryServices();
        services.clearTableRegionCache(tableNameBytes);

        long renewScannerLeaseThreshold = queryPlan.getContext().getConnection().getQueryServices().getRenewLeaseThresholdMilliSeconds();
        boolean isRequestMetricsEnabled = readMetrics.isRequestMetricsEnabled();
        for (Scan scan : scans) {
            // For MR, skip the region boundary check exception if we encounter a split. ref: PHOENIX-2599
            scan.setAttribute(BaseScannerRegionObserver.SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));

            PeekingResultIterator peekingResultIterator;
            ScanMetricsHolder scanMetricsHolder =
                    ScanMetricsHolder.getInstance(readMetrics, tableName, scan,
                            isRequestMetricsEnabled);
            if (snapshotName != null) {
                // result iterator to read snapshots
                final TableSnapshotResultIterator tableSnapshotResultIterator = new TableSnapshotResultIterator(conf, scan,
                        scanMetricsHolder);
                peekingResultIterator = LookAheadResultIterator.wrap(tableSnapshotResultIterator);
            } else {
                final TableResultIterator tableResultIterator =
                        new TableResultIterator(
                                queryPlan.getContext().getConnection().getMutationState(), scan,
                                scanMetricsHolder, renewScannerLeaseThreshold, queryPlan,
                                MapReduceParallelScanGrouper.getInstance());
                peekingResultIterator = LookAheadResultIterator.wrap(tableResultIterator);
            }

            iterators.add(peekingResultIterator);
        }
        ResultIterator iterator = queryPlan.useRoundRobinIterator() ? RoundRobinResultIterator.newIterator(iterators, queryPlan) : ConcatResultIterator.newIterator(iterators);
        if(queryPlan.getContext().getSequenceManager().getSequenceCount() > 0) {
            iterator = new SequenceResultIterator(iterator, queryPlan.getContext().getSequenceManager());
        }
        return iterator;
    }

    public static PhoenixResultSet initializeResultSet(ResultIterator resultIterator,
            QueryPlan queryPlan) throws SQLException {
        // Clone the row projector as it's not thread safe and would be used simultaneously by
        // multiple threads otherwise.
        return new PhoenixResultSet(resultIterator, queryPlan.getProjector().cloneIfNecessary(),
                queryPlan.getContext());
    }

    public static List<InputSplit> generateMultiTableSplits(QueryPlan sourcePlan,
            QueryPlan targetPlan, Configuration conf) throws IOException {
        Preconditions.checkNotNull(sourcePlan);
        Preconditions.checkNotNull(targetPlan);
        List<InputSplit> sourceSplits = generateSplits(sourcePlan, sourcePlan.getSplits(), conf);
        List<List<Scan>> targetScans = targetPlan.getScans();
        List<InputSplit> multiTableSplits = Lists.newArrayListWithExpectedSize(sourceSplits.size());
        for (int i = 0; i < sourceSplits.size(); i++) {
            PhoenixInputSplit split = (PhoenixInputSplit) sourceSplits.get(i);
            List<Scan> sourceScans = split.getScans();
            List<Scan> alignedScans = Lists.newArrayListWithExpectedSize(sourceScans.size());
            Scan targetScan = targetScans.get(i).get(0);
            Scan s = new Scan(targetScan);
            for (Scan sourceScan : sourceScans) {
                targetScan.setStartRow(sourceScan.getStartRow());
                targetScan.setStopRow(sourceScan.getStopRow());
                alignedScans.add(targetScan);
            }
            multiTableSplits.add(new MultiTableInputSplit(split, alignedScans));
        }
        return multiTableSplits;
    }

}

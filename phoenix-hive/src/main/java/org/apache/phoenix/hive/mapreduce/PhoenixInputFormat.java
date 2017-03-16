/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive.mapreduce;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.ppd.PhoenixPredicateDecomposer;
import org.apache.phoenix.hive.ql.index.IndexSearchCondition;
import org.apache.phoenix.hive.query.PhoenixQueryBuilder;
import org.apache.phoenix.hive.util.PhoenixConnectionUtil;
import org.apache.phoenix.hive.util.PhoenixStorageHandlerUtil;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.PhoenixRuntime;

/**
 * Custom InputFormat to feed into Hive
 */
@SuppressWarnings({"deprecation", "rawtypes"})
public class PhoenixInputFormat<T extends DBWritable> implements InputFormat<WritableComparable,
        T> {

    private static final Log LOG = LogFactory.getLog(PhoenixInputFormat.class);

    public PhoenixInputFormat() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("PhoenixInputFormat created");
        }
    }

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        String tableName = jobConf.get(PhoenixStorageHandlerConstants.PHOENIX_TABLE_NAME);

        String query;
        String executionEngine = jobConf.get(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname,
                HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.getDefaultValue());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Target table name at split phase : " + tableName + "with whereCondition :" +
                    jobConf.get(TableScanDesc.FILTER_TEXT_CONF_STR) +
                    " and " + HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname + " : " +
                    executionEngine);
        }

        if (PhoenixStorageHandlerConstants.MR.equals(executionEngine)) {
            List<IndexSearchCondition> conditionList = null;
            String filterExprSerialized = jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
            if (filterExprSerialized != null) {
                ExprNodeGenericFuncDesc filterExpr =
                        Utilities.deserializeExpression(filterExprSerialized);
                PhoenixPredicateDecomposer predicateDecomposer =
                        PhoenixPredicateDecomposer.create(Arrays.asList(jobConf.get(serdeConstants.LIST_COLUMNS).split(",")));
                predicateDecomposer.decomposePredicate(filterExpr);
                if (predicateDecomposer.isCalledPPD()) {
                    conditionList = predicateDecomposer.getSearchConditionList();
                }
            }

            query = PhoenixQueryBuilder.getInstance().buildQuery(jobConf, tableName,
                    PhoenixStorageHandlerUtil.getReadColumnNames(jobConf), conditionList);
        } else if (PhoenixStorageHandlerConstants.TEZ.equals(executionEngine)) {
            Map<String, TypeInfo> columnTypeMap =
                    PhoenixStorageHandlerUtil.createColumnTypeMap(jobConf);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Column type map for TEZ : " + columnTypeMap);
            }

            String whereClause = jobConf.get(TableScanDesc.FILTER_TEXT_CONF_STR);
            query = PhoenixQueryBuilder.getInstance().buildQuery(jobConf, tableName,
                    PhoenixStorageHandlerUtil.getReadColumnNames(jobConf), whereClause, columnTypeMap);
        } else {
            throw new IOException(executionEngine + " execution engine unsupported yet.");
        }

        final QueryPlan queryPlan = getQueryPlan(jobConf, query);
        final List<KeyRange> allSplits = queryPlan.getSplits();
        final List<InputSplit> splits = generateSplits(jobConf, queryPlan, allSplits, query);

        return splits.toArray(new InputSplit[splits.size()]);
    }

    private List<InputSplit> generateSplits(final JobConf jobConf, final QueryPlan qplan,
                                            final List<KeyRange> splits, String query) throws
            IOException {
        Preconditions.checkNotNull(qplan);
        Preconditions.checkNotNull(splits);
        final List<InputSplit> psplits = Lists.newArrayListWithExpectedSize(splits.size());

        Path[] tablePaths = FileInputFormat.getInputPaths(ShimLoader.getHadoopShims()
                .newJobContext(new Job(jobConf)));
        boolean splitByStats = jobConf.getBoolean(PhoenixStorageHandlerConstants.SPLIT_BY_STATS,
                false);

        setScanCacheSize(jobConf);

        // Adding Localization
        HConnection connection = HConnectionManager.createConnection(PhoenixConnectionUtil.getConfiguration(jobConf));
        RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(qplan
                .getTableRef().getTable().getPhysicalName().toString()));
        RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(regionLocator, connection
                .getAdmin());

        for (List<Scan> scans : qplan.getScans()) {
            PhoenixInputSplit inputSplit;

            HRegionLocation location = regionLocator.getRegionLocation(scans.get(0).getStartRow()
                    , false);
            long regionSize = sizeCalculator.getRegionSize(location.getRegionInfo().getRegionName
                    ());
            String regionLocation = PhoenixStorageHandlerUtil.getRegionLocation(location, LOG);

            if (splitByStats) {
                for (Scan aScan : scans) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Split for  scan : " + aScan + "with scanAttribute : " + aScan
                                .getAttributesMap() + " [scanCache, cacheBlock, scanBatch] : [" +
                                aScan.getCaching() + ", " + aScan.getCacheBlocks() + ", " + aScan
                                .getBatch() + "] and  regionLocation : " + regionLocation);
                    }

                    inputSplit = new PhoenixInputSplit(Lists.newArrayList(aScan), tablePaths[0],
                            regionLocation, regionSize);
                    inputSplit.setQuery(query);
                    psplits.add(inputSplit);
                }
            } else {
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

                inputSplit = new PhoenixInputSplit(scans, tablePaths[0], regionLocation,
                        regionSize);
                inputSplit.setQuery(query);
                psplits.add(inputSplit);
            }
        }

        return psplits;
    }

    private void setScanCacheSize(JobConf jobConf) {
        int scanCacheSize = jobConf.getInt(PhoenixStorageHandlerConstants.HBASE_SCAN_CACHE, -1);
        if (scanCacheSize > 0) {
            jobConf.setInt(HConstants.HBASE_CLIENT_SCANNER_CACHING, scanCacheSize);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Generating splits with scanCacheSize : " + scanCacheSize);
        }
    }

    @Override
    public RecordReader<WritableComparable, T> getRecordReader(InputSplit split, JobConf job,
                                                               Reporter reporter) throws
            IOException {
        final QueryPlan queryPlan = getQueryPlan(job, ((PhoenixInputSplit) split).getQuery());
        @SuppressWarnings("unchecked")
        final Class<T> inputClass = (Class<T>) job.getClass(PhoenixConfigurationUtil.INPUT_CLASS,
                PhoenixResultWritable.class);

        PhoenixRecordReader<T> recordReader = new PhoenixRecordReader<T>(inputClass, job,
                queryPlan);
        recordReader.initialize(split);

        return recordReader;
    }

    /**
     * Returns the query plan associated with the select query.
     */
    private QueryPlan getQueryPlan(final Configuration configuration, String selectStatement)
            throws IOException {
        try {
            final String currentScnValue = configuration.get(PhoenixConfigurationUtil
                    .CURRENT_SCN_VALUE);
            final Properties overridingProps = new Properties();
            if (currentScnValue != null) {
                overridingProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, currentScnValue);
            }
            final Connection connection = PhoenixConnectionUtil.getInputConnection(configuration,
                    overridingProps);
            Preconditions.checkNotNull(selectStatement);
            final Statement statement = connection.createStatement();
            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Compiled query : " + selectStatement);
            }

            // Optimize the query plan so that we potentially use secondary indexes
            final QueryPlan queryPlan = pstmt.optimizeQuery(selectStatement);
            // Initialize the query plan so it sets up the parallel scans
            queryPlan.iterator(MapReduceParallelScanGrouper.getInstance());
            return queryPlan;
        } catch (Exception exception) {
            LOG.error(String.format("Failed to get the query plan with error [%s]", exception.getMessage()));
            throw new RuntimeException(exception);
        }
    }
}

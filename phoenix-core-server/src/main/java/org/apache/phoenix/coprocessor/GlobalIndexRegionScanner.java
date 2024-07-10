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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.filter.EmptyColumnOnlyFilter;
import org.apache.phoenix.filter.PagingFilter;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.filter.AllVersionsIndexRebuildFilter;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.parallel.EarlyExitFailure;
import org.apache.phoenix.hbase.index.parallel.TaskBatch;
import org.apache.phoenix.hbase.index.parallel.TaskRunner;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolBuilder;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolManager;
import org.apache.phoenix.hbase.index.parallel.WaitForCompletionTaskRunner;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.transform.TransformMaintainer;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.apache.phoenix.hbase.index.write.AbstractParallelWriterIndexCommitter.INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY;
import static org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.IndexVerificationErrorType.BEYOND_MAX_LOOKBACK_INVALID;
import static org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.IndexVerificationErrorType.BEYOND_MAX_LOOKBACK_MISSING;
import static org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.IndexVerificationErrorType.EXTRA_CELLS;
import static org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.IndexVerificationErrorType.EXTRA_ROW;
import static org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.IndexVerificationErrorType.INVALID_ROW;
import static org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.IndexVerificationErrorType.MISSING_ROW;
import static org.apache.phoenix.query.QueryConstants.UNVERIFIED_BYTES;
import static org.apache.phoenix.query.QueryConstants.VERIFIED_BYTES;
import static org.apache.phoenix.query.QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB;
import static org.apache.phoenix.util.ScanUtil.isDummy;

/**
 * This is an abstract region scanner which is used to scan index or data table rows locally. From the data table rows,
 * expected index table mutations are generated. These expected index mutations are used for both repairing and
 * rebuilding index table rows and also verifying them.
 */
public abstract class GlobalIndexRegionScanner extends BaseRegionScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalIndexRegionScanner.class);

    public static final String NUM_CONCURRENT_INDEX_VERIFY_THREADS_CONF_KEY = "phoenix.index.verify.threads.max";
    public static final int DEFAULT_CONCURRENT_INDEX_VERIFY_THREADS = 16;
    public static final String INDEX_VERIFY_ROW_COUNTS_PER_TASK_CONF_KEY = "phoenix.index.verify.row.count.per.task";
    public static final int DEFAULT_INDEX_VERIFY_ROW_COUNTS_PER_TASK = 2048;
    public static final String NO_EXPECTED_MUTATION = "No expected mutation";
    public static final String ACTUAL_MUTATION_IS_NULL_OR_EMPTY = "actualMutationList is null or empty";
    public static final String ERROR_MESSAGE_MISSING_INDEX_ROW_BEYOND_MAX_LOOKBACK = "Missing index row beyond maxLookBack";
    public static final String ERROR_MESSAGE_MISSING_INDEX_ROW = "Missing index row";
    public static final String ERROR_MESSAGE_EXTRA_INDEX_ROW = "Extra index row";
    public static final String PHOENIX_INDEX_MR_LOG_BEYOND_MAX_LOOKBACK_ERRORS =
            "phoenix.index.mr.log.beyond.max.lookback.errors";
    public static final boolean DEFAULT_PHOENIX_INDEX_MR_LOG_BEYOND_MAX_LOOKBACK_ERRORS = false;

    protected final UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver;

    protected IndexTool.IndexDisableLoggingType disableLoggingVerifyType = IndexTool.IndexDisableLoggingType.NONE;
    protected byte[][] viewConstants;
    protected IndexVerificationOutputRepository verificationOutputRepository = null;
    protected boolean skipped = false;
    protected boolean shouldRetry = false;
    protected boolean shouldVerifyCheckDone = false;
    final protected RegionCoprocessorEnvironment env;
    protected byte[][] regionEndKeys;
    protected byte[] nextStartKey;
    protected boolean hasMoreIncr = false;
    protected long minTimestamp = 0;
    protected byte[] indexRowKeyforReadRepair;
    protected Table dataHTable = null;
    protected long pageSizeInRows = Long.MAX_VALUE;
    protected int rowCountPerTask;
    protected boolean hasMore;
    protected int maxBatchSize;
    protected final long maxBatchSizeBytes;
    protected final long blockingMemstoreSize;
    protected final byte[] clientVersionBytes;
    protected boolean useProto = true;
    protected byte[] indexMetaData;
    protected Scan scan;
    protected RegionScanner innerScanner;
    protected Region region;
    protected IndexMaintainer indexMaintainer;
    protected Table indexHTable = null;
    protected TaskRunner pool;
    protected String exceptionMessage;
    protected HTableFactory hTableFactory;
    protected int indexTableTTL;
    protected long maxLookBackInMills;
    protected IndexToolVerificationResult verificationResult = null;
    protected IndexVerificationResultRepository verificationResultRepository = null;
    protected Map<byte[], NavigableSet<byte[]>> familyMap;
    protected IndexTool.IndexVerifyType verifyType = IndexTool.IndexVerifyType.NONE;
    protected boolean verify = false;
    protected byte[] tenantId;
    protected byte[] schemaName;
    protected byte[] logicalTableName;
    protected byte[] tableType;
    protected byte[] lastDdlTimestamp;

    // This relies on Hadoop Configuration to handle warning about deprecated configs and
    // to set the correct non-deprecated configs when an old one shows up.
    static {
        Configuration.addDeprecation("index.verify.threads.max", NUM_CONCURRENT_INDEX_VERIFY_THREADS_CONF_KEY);
        Configuration.addDeprecation("index.verify.row.count.per.task", INDEX_VERIFY_ROW_COUNTS_PER_TASK_CONF_KEY);
    }

    public GlobalIndexRegionScanner(final RegionScanner innerScanner,
                                    final Region region,
                                    final Scan scan,
                                    final RegionCoprocessorEnvironment env,
                                    final UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver)
            throws IOException {
        super(innerScanner);
        final Configuration config = env.getConfiguration();
        if (scan.getAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_PAGING) != null) {
            byte[] pageSizeFromScan =
                    scan.getAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_PAGE_ROWS);
            if (pageSizeFromScan != null) {
                pageSizeInRows = Bytes.toLong(pageSizeFromScan);
            } else {
                pageSizeInRows =
                        config.getLong(INDEX_REBUILD_PAGE_SIZE_IN_ROWS,
                            QueryServicesOptions.DEFAULT_INDEX_REBUILD_PAGE_SIZE_IN_ROWS);
            }
        }
        maxBatchSize = config.getInt(MUTATE_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
        maxBatchSizeBytes = config.getLongBytes(MUTATE_BATCH_SIZE_BYTES_ATTRIB,
                QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE_BYTES);
        blockingMemstoreSize = UngroupedAggregateRegionObserver.getBlockingMemstoreSize(region, config);
        clientVersionBytes = scan.getAttribute(BaseScannerRegionObserverConstants.CLIENT_VERSION);
        familyMap = scan.getFamilyMap();
        if (familyMap.isEmpty()) {
            familyMap = null;
        }
        indexMetaData = scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD);
        if (indexMetaData == null) {
            indexMetaData = scan.getAttribute(PhoenixIndexCodec.INDEX_MD);
        }
        tenantId = scan.getAttribute(MutationState.MutationMetadataType.TENANT_ID.toString());
        schemaName = scan.getAttribute(MutationState.MutationMetadataType.SCHEMA_NAME.toString());
        logicalTableName = scan.getAttribute(
                MutationState.MutationMetadataType.LOGICAL_TABLE_NAME.toString());
        tableType = scan.getAttribute(MutationState.MutationMetadataType.TABLE_TYPE.toString());
        lastDdlTimestamp = scan.getAttribute(
                MutationState.MutationMetadataType.TIMESTAMP.toString());
        byte[] transforming = scan.getAttribute(BaseScannerRegionObserverConstants.DO_TRANSFORMING);
        List<IndexMaintainer> maintainers = null;
        if (transforming == null) {
            maintainers = IndexMaintainer.deserialize(indexMetaData, true);
        } else {
            maintainers = TransformMaintainer.deserialize(indexMetaData);
        }
        indexMaintainer = maintainers.get(0);
        this.scan = scan;
        this.innerScanner = innerScanner;
        this.region = region;
        this.env = env;
        this.ungroupedAggregateRegionObserver = ungroupedAggregateRegionObserver;
        byte[] valueBytes = scan.getAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_VERIFY_TYPE);
        if (valueBytes != null) {
            verifyType = IndexTool.IndexVerifyType.fromValue(valueBytes);
            if (verifyType != IndexTool.IndexVerifyType.NONE) {
                verify = true;
            }
        }
        // Create the following objects only for rebuilds by IndexTool
        hTableFactory = IndexWriterUtils.getDefaultDelegateHTableFactory(env);
        maxLookBackInMills = BaseScannerRegionObserverConstants.getMaxLookbackInMillis(config);
        rowCountPerTask = config.getInt(INDEX_VERIFY_ROW_COUNTS_PER_TASK_CONF_KEY,
                DEFAULT_INDEX_VERIFY_ROW_COUNTS_PER_TASK);

        pool = new WaitForCompletionTaskRunner(ThreadPoolManager.getExecutor(
                new ThreadPoolBuilder("IndexVerify",
                        env.getConfiguration()).setMaxThread(NUM_CONCURRENT_INDEX_VERIFY_THREADS_CONF_KEY,
                        DEFAULT_CONCURRENT_INDEX_VERIFY_THREADS).setCoreTimeout(
                        INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY), env));

        if (verify) {
            boolean shouldLogBeyondMaxLookbackInvalidRows;
            byte[] scanParamShouldLogBeyondMaxLookbackInvalidRows =
                    scan.getAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_DISABLE_LOGGING_BEYOND_MAXLOOKBACK_AGE);
            if (scanParamShouldLogBeyondMaxLookbackInvalidRows != null) {
                shouldLogBeyondMaxLookbackInvalidRows =
                        Boolean.parseBoolean(Bytes.toString(scanParamShouldLogBeyondMaxLookbackInvalidRows));
            } else {
                shouldLogBeyondMaxLookbackInvalidRows =
                        env.getConfiguration().getBoolean(PHOENIX_INDEX_MR_LOG_BEYOND_MAX_LOOKBACK_ERRORS,
                                DEFAULT_PHOENIX_INDEX_MR_LOG_BEYOND_MAX_LOOKBACK_ERRORS);
            }
            viewConstants = IndexUtil.deserializeViewConstantsFromScan(scan);
            byte[] disableLoggingValueBytes =
                    scan.getAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_DISABLE_LOGGING_VERIFY_TYPE);
            if (disableLoggingValueBytes != null) {
                disableLoggingVerifyType =
                        IndexTool.IndexDisableLoggingType.fromValue(disableLoggingValueBytes);
            }
            verificationOutputRepository =
                    new IndexVerificationOutputRepository(indexMaintainer.getIndexTableName()
                            , hTableFactory, disableLoggingVerifyType);
            verificationOutputRepository.setShouldLogBeyondMaxLookback(shouldLogBeyondMaxLookbackInvalidRows);
            verificationResult = new IndexToolVerificationResult(scan);
            verificationResultRepository =
                    new IndexVerificationResultRepository(indexMaintainer.getIndexTableName(), hTableFactory);
            nextStartKey = null;
            minTimestamp = scan.getTimeRange().getMin();
        }
    }


    public static long getTimestamp(Mutation m) {
        for (List<Cell> cells : m.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                return cell.getTimestamp();
            }
        }
        throw new IllegalStateException("No cell found");
    }

    protected static boolean isTimestampBeforeTTL(long tableTTL, long currentTime, long tsToCheck) {
        if (tableTTL == HConstants.FOREVER) {
            return false;
        }
        return tsToCheck < (currentTime - tableTTL * 1000);
    }

    protected static boolean isTimestampBeyondMaxLookBack(long maxLookBackInMills,
            long currentTime, long tsToCheck) {
        if (!BaseScannerRegionObserver.isMaxLookbackTimeEnabled(maxLookBackInMills)) {
            // By definition, if the max lookback feature is not enabled, then delete markers and rows
            // version can be removed by compaction any time, and thus there is no window in which these mutations are
            // preserved, i.e.,  the max lookback window size is zero. This means all the mutations are effectively
            // beyond the zero size max lookback window.
            return true;
        }
        return tsToCheck < (currentTime - maxLookBackInMills);
    }

    protected boolean isColumnIncluded(Cell cell) {
        byte[] family = CellUtil.cloneFamily(cell);
        if (!familyMap.containsKey(family)) {
            return false;
        }
        NavigableSet<byte[]> set = familyMap.get(family);
        if (set == null || set.isEmpty()) {
            return true;
        }
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        return set.contains(qualifier);
    }

    @VisibleForTesting
    public boolean shouldVerify(IndexTool.IndexVerifyType verifyType,
                                byte[] indexRowKey, Scan scan, Region region, IndexMaintainer indexMaintainer,
                                IndexVerificationResultRepository verificationResultRepository, boolean shouldVerifyCheckDone) throws IOException {
        this.verifyType = verifyType;
        this.indexRowKeyforReadRepair = indexRowKey;
        this.scan = scan;
        this.region = region;
        this.indexMaintainer = indexMaintainer;
        this.verificationResultRepository = verificationResultRepository;
        this.shouldVerifyCheckDone = shouldVerifyCheckDone;
        return shouldVerify();
    }

    public boolean shouldVerify() throws IOException {
        // In case of read repair, proceed with rebuild
        // All other types of rebuilds/verification should be incrementally performed if appropriate param is passed
        byte[] lastVerifyTimeValue = scan.getAttribute(BaseScannerRegionObserverConstants.INDEX_RETRY_VERIFY);
        Long lastVerifyTime = lastVerifyTimeValue == null ? 0 : Bytes.toLong(lastVerifyTimeValue);
        if(indexRowKeyforReadRepair != null || lastVerifyTime == 0 || shouldVerifyCheckDone) {
            return true;
        }

        IndexToolVerificationResult verificationResultTemp = verificationResultRepository
            .getVerificationResult(lastVerifyTime, scan, region, indexMaintainer.getIndexTableName());
        if(verificationResultTemp != null) {
            verificationResult = verificationResultTemp;
        }

        shouldVerifyCheckDone = true;
        if (verificationResult != null && verificationResult.getShouldRetry()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("ShouldRetry is true. " + region.getRegionInfo().getRegionNameAsString());
            }
            return true;
        }
        return verificationResultTemp == null;
    }

    @Override
    public RegionInfo getRegionInfo() {
        return region.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() {
        return false;
    }

    private void closeTables() throws IOException {
        hTableFactory.shutdown();
        if (indexHTable != null) {
            indexHTable.close();
        }
        if (dataHTable != null) {
            dataHTable.close();
        }
    }

    @Override
    public void close() throws IOException {
        innerScanner.close();
        if (indexRowKeyforReadRepair != null) {
            closeTables();
            return;
        }
        if (verify) {
            try {
                if (verificationResultRepository != null) {
                    verificationResultRepository.logToIndexToolResultTable(verificationResult,
                            verifyType, region.getRegionInfo().getRegionName(), skipped, shouldRetry);
                }
            } finally {
                this.pool.stop("IndexRegionObserverRegionScanner is closing");
                closeTables();
                if (verificationResultRepository != null) {
                    verificationResultRepository.close();
                }
                if (verificationOutputRepository != null) {
                    verificationOutputRepository.close();
                }
            }
        }
        else {
            this.pool.stop("GlobalIndexRegionScanner is closing");
            closeTables();
        }
    }

    @VisibleForTesting
    public int setIndexTableTTL(int ttl) {
        indexTableTTL = ttl;
        return 0;
    }

    @VisibleForTesting
    public int setIndexMaintainer(IndexMaintainer indexMaintainer) {
        this.indexMaintainer = indexMaintainer;
        return 0;
    }

    @VisibleForTesting
    public long setMaxLookBackInMills(long maxLookBackInMills) {
        this.maxLookBackInMills = maxLookBackInMills;
        return 0;
    }

    public void logToIndexToolOutputTable(byte[] dataRowKey, byte[] indexRowKey, long dataRowTs, long indexRowTs,
                                          String errorMsg, boolean isBeforeRebuild,
                                          IndexVerificationOutputRepository.IndexVerificationErrorType errorType) throws IOException {
        logToIndexToolOutputTable(dataRowKey, indexRowKey, dataRowTs, indexRowTs, errorMsg, null,
                null, isBeforeRebuild, errorType);
    }

    protected byte[] getDataTableName() {
        return region.getRegionInfo().getTable().getName();
    }

    @VisibleForTesting
    public void logToIndexToolOutputTable(byte[] dataRowKey, byte[] indexRowKey, long dataRowTs, long indexRowTs,
                                          String errorMsg, byte[] expectedVaue, byte[] actualValue, boolean isBeforeRebuild,
                                          IndexVerificationOutputRepository.IndexVerificationErrorType errorType) throws IOException {
        ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
        byte[] dataTableName = getDataTableName();
        verificationOutputRepository.logToIndexToolOutputTable(dataRowKey, indexRowKey, dataRowTs, indexRowTs,
                errorMsg, expectedVaue, actualValue, scan.getTimeRange().getMax(),
                dataTableName, isBeforeRebuild, errorType);
    }

    private static Cell getCell(Mutation m, byte[] family, byte[] qualifier) {
        List<Cell> cellList = m.getFamilyCellMap().get(family);
        if (cellList == null) {
            return null;
        }
        for (Cell cell : cellList) {
            if (CellUtil.matchingQualifier(cell, qualifier)) {
                return cell;
            }
        }
        return null;
    }

    private void logMismatch(Mutation expected, Mutation actual, int iteration, IndexToolVerificationResult.PhaseResult verificationPhaseResult, boolean isBeforeRebuild) throws IOException {
        if (getTimestamp(expected) != getTimestamp(actual)) {
            String errorMsg = "Not matching timestamp";
            byte[] dataKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(expected.getRow()), viewConstants);
            logToIndexToolOutputTable(dataKey, expected.getRow(), getTimestamp(expected), getTimestamp(actual),
                    errorMsg, null, null, isBeforeRebuild, INVALID_ROW);
            return;
        }
        int expectedCellCount = 0;
        for (List<Cell> cells : expected.getFamilyCellMap().values()) {
            if (cells == null) {
                continue;
            }
            for (Cell expectedCell : cells) {
                expectedCellCount++;
                byte[] family = CellUtil.cloneFamily(expectedCell);
                byte[] qualifier = CellUtil.cloneQualifier(expectedCell);
                Cell actualCell = getCell(actual, family, qualifier);
                if (actualCell == null || expectedCell.getType() != actualCell.getType()) {
                    byte[] dataKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(expected.getRow()), viewConstants);
                    String errorMsg = "Missing cell (in iteration " + iteration + ") " + Bytes.toString(family) + ":" + Bytes.toString(qualifier);
                    logToIndexToolOutputTable(dataKey, expected.getRow(), getTimestamp(expected),
                            getTimestamp(actual), errorMsg, isBeforeRebuild, INVALID_ROW);
                    verificationPhaseResult.setIndexHasMissingCellsCount(verificationPhaseResult.getIndexHasMissingCellsCount() + 1);
                    return;
                }
                if (!CellUtil.matchingValue(actualCell, expectedCell)) {
                    String errorMsg = "Not matching value (in iteration " + iteration + ") for " + Bytes.toString(family) + ":" + Bytes.toString(qualifier);
                    byte[] dataKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(expected.getRow()), viewConstants);
                    logToIndexToolOutputTable(dataKey, expected.getRow(), getTimestamp(expected), getTimestamp(actual),
                            errorMsg, CellUtil.cloneValue(expectedCell),
                            CellUtil.cloneValue(actualCell), isBeforeRebuild, INVALID_ROW);
                    return;
                }
            }
        }
        int actualCellCount = 0;
        for (List<Cell> cells : actual.getFamilyCellMap().values()) {
            if (cells == null) {
                continue;
            }
            actualCellCount += cells.size();
        }
        if (expectedCellCount != actualCellCount) {
            String errorMsg = "Index has extra cells (in iteration " + iteration + ")";
            byte[] dataKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(expected.getRow()), viewConstants);
            logToIndexToolOutputTable(dataKey, expected.getRow(), getTimestamp(expected), getTimestamp(actual),
                    errorMsg, isBeforeRebuild, EXTRA_CELLS);
            verificationPhaseResult.setIndexHasExtraCellsCount(verificationPhaseResult.getIndexHasExtraCellsCount() + 1);
        }
    }

    private boolean isMatchingMutation(Mutation expected, Mutation actual) {
        if (getTimestamp(expected) != getTimestamp(actual)) {
            return false;
        }
        int expectedCellCount = 0;
        for (List<Cell> cells : expected.getFamilyCellMap().values()) {
            if (cells == null) {
                continue;
            }
            for (Cell expectedCell : cells) {
                expectedCellCount++;
                byte[] family = CellUtil.cloneFamily(expectedCell);
                byte[] qualifier = CellUtil.cloneQualifier(expectedCell);
                Cell actualCell = getCell(actual, family, qualifier);
                if (actualCell == null ||
                        expectedCell.getType() != actualCell.getType()) {
                    return false;
                }
                if (!CellUtil.matchingValue(actualCell, expectedCell)) {
                    return false;
                }
            }
        }
        int actualCellCount = 0;
        for (List<Cell> cells : actual.getFamilyCellMap().values()) {
            if (cells == null) {
                continue;
            }
            actualCellCount += cells.size();
        }
        if (expectedCellCount != actualCellCount) {
            return false;
        }
        return true;
    }

    private boolean isVerified(Put mutation) throws IOException {
        List<Cell> cellList = mutation.get(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                indexMaintainer.getEmptyKeyValueQualifier());
        Cell cell = (cellList != null && !cellList.isEmpty()) ? cellList.get(0) : null;
        if (cell == null) {
            return false;
        }
        if (Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                VERIFIED_BYTES, 0, VERIFIED_BYTES.length) == 0) {
            return true;
        }
        return false;
    }

    /**
     * This is to reorder the mutations in descending order by the tuple of timestamp and mutation type where
     * delete comes before put
     */
    public static final Comparator<Mutation> MUTATION_TS_DESC_COMPARATOR = new Comparator<Mutation>() {
        @Override
        public int compare(Mutation o1, Mutation o2) {
            long ts1 = getTimestamp(o1);
            long ts2 = getTimestamp(o2);
            if (ts1 > ts2) {
                return -1;
            }
            if (ts1 < ts2) {
                return 1;
            }
            if (o1 instanceof Delete && o2 instanceof Put) {
                return -1;
            }
            if (o1 instanceof Put && o2 instanceof Delete) {
                return 1;
            }
            return 0;
        }
    };

    private boolean isDeleteFamily(Mutation mutation) {
        for (List<Cell> cells : mutation.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                if (cell.getType() == Cell.Type.DeleteFamily) {
                    return true;
                }
            }
        }
        return false;
    }

    private void updateUnverifiedIndexRowCounters(Put actual, long expectedTs, List<Mutation> indexRowsToBeDeleted,
                                                  IndexToolVerificationResult.PhaseResult verificationPhaseResult) {
        // Get the empty column of the given index row
        List<Cell> cellList = actual.get(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                indexMaintainer.getEmptyKeyValueQualifier());
        Cell cell = (cellList != null && !cellList.isEmpty()) ? cellList.get(0) : null;
        if (cell == null) {
            // There is no empty column on the given index row. We do not know if this is a row generated by the new
            // or the old design
            verificationPhaseResult.setUnknownIndexRowCount(verificationPhaseResult.getUnknownIndexRowCount() + 1);
            return;
        }
        if (Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                VERIFIED_BYTES, 0, VERIFIED_BYTES.length) == 0) {
            // This is a verified index row, so nothing to do here
            return;
        } else if (Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                UNVERIFIED_BYTES, 0, UNVERIFIED_BYTES.length) == 0) {
            verificationPhaseResult.setUnverifiedIndexRowCount(verificationPhaseResult.getUnverifiedIndexRowCount() +  1);
            return;
        }
        // The empty column value is neither "verified" or "unverified". This must be a row from the old design
        verificationPhaseResult.setOldIndexRowCount(verificationPhaseResult.getOldIndexRowCount() + 1);
        if (verifyType == IndexTool.IndexVerifyType.BEFORE || verifyType == IndexTool.IndexVerifyType.BOTH) {
            long actualTs = getTimestamp(actual);
            if (actualTs > expectedTs) {
                indexRowsToBeDeleted.add(indexMaintainer.buildRowDeleteMutation(actual.getRow(),
                        IndexMaintainer.DeleteType.SINGLE_VERSION, actualTs));
            }
        }
    }

    /**
     * actualIndexMutationList is the list of all the mutations of a single extra index row (i.e. not referenced by data row)
     * ordered by decreasing order of timestamps with Deletes before Puts
     */
    private void logExtraIndexRowAndUpdateCounters(List<Mutation> actualIndexMutationList,
        IndexToolVerificationResult.PhaseResult verificationPhaseResult, boolean isBeforeRebuild) throws IOException {
        for (Mutation m : actualIndexMutationList) {
            // this extra row in the index table has already been deleted
            if ((m instanceof Delete)) {
                return;
            }

            // check the empty column status of latest (most recent) put mutation
            if (isVerified((Put) m)) {
                verificationPhaseResult.setExtraVerifiedIndexRowCount(
                    verificationPhaseResult.getExtraVerifiedIndexRowCount() + 1);
            } else {
                verificationPhaseResult.setExtraUnverifiedIndexRowCount(
                    verificationPhaseResult.getExtraUnverifiedIndexRowCount() + 1);
            }

            byte[] indexKey = m.getRow();
            byte[] dataKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(indexKey), viewConstants);
            String errorMsg = ERROR_MESSAGE_EXTRA_INDEX_ROW;
            IndexVerificationOutputRepository.IndexVerificationErrorType errorType = EXTRA_ROW;
            logToIndexToolOutputTable(dataKey, indexKey, 0, getTimestamp(m), errorMsg,
                isBeforeRebuild, errorType);
            break;
        }
    }

    /**
     * In this method, the actual list is repaired in memory using the expected list which is actually the output of
     * rebuilding the index table row. The result of this repair is used only for verification.
     */
    private void repairActualMutationList(List<Mutation> actualMutationList, List<Mutation> expectedMutationList)
            throws IOException {
        // Find the first (latest) actual unverified put mutation
        List<Mutation> repairedMutationList = new ArrayList<>(expectedMutationList.size());
        for (Mutation actual : actualMutationList) {
            if (actual instanceof Put && !isVerified((Put) actual)) {
                long ts = getTimestamp(actual);
                int expectedIndex;
                int expectedListSize = expectedMutationList.size();
                for (expectedIndex = 0; expectedIndex < expectedListSize; expectedIndex++) {
                    if (getTimestamp(expectedMutationList.get(expectedIndex)) <= ts) {
                        if (expectedIndex > 0) {
                            expectedIndex--;
                        }
                        break;
                    }
                }
                if (expectedIndex == expectedListSize) {
                    continue;
                }
                for (; expectedIndex < expectedListSize; expectedIndex++) {
                    Mutation mutation = expectedMutationList.get(expectedIndex);
                    if (mutation instanceof Put) {
                        mutation = new Put((Put) mutation);
                    } else {
                        mutation = new Delete((Delete) mutation);
                    }
                    repairedMutationList.add(mutation);
                }
                // Since we repair the entire history, there is no need to more than once
                break;
            }
        }
        if (repairedMutationList.isEmpty()) {
            return;
        }
        actualMutationList.addAll(repairedMutationList);
        Collections.sort(actualMutationList, MUTATION_TS_DESC_COMPARATOR);
    }

    private void cleanUpActualMutationList(List<Mutation> actualMutationList)
            throws IOException {
        Iterator<Mutation> iterator = actualMutationList.iterator();
        Mutation previous = null;
        while (iterator.hasNext()) {
            Mutation mutation = iterator.next();
            if ((mutation instanceof Put && !isVerified((Put) mutation)) ||
                    (mutation instanceof Delete && !isDeleteFamily(mutation))) {
                iterator.remove();
            } else {
                if (((previous instanceof Put && mutation instanceof Put) ||
                        previous instanceof Delete && mutation instanceof Delete) &&
                        isMatchingMutation(previous, mutation)) {
                    iterator.remove();
                } else {
                    previous = mutation;
                }
            }
        }
    }

    /**
     * There are two types of verification: without repair and with repair. Without-repair verification is done before
     * or after index rebuild. It is done before index rebuild to identify the rows to be rebuilt. It is done after
     * index rebuild to verify the rows that have been rebuilt. With-repair verification can be done anytime using
     * the “-v ONLY” option to check the consistency of the index table. Note that with-repair verification simulates
     * read repair in-memory for the purpose of verification, but does not actually repair the data in the index.
     *
     * Unverified Rows
     *
     * For each mutable data table mutation during regular data table updates, two operations are done on the data table.
     * One is to read the existing row state, and the second is to update the data table for this row. The processing of
     * concurrent data mutations are serialized once for reading the existing row states, and then serialized again
     * for updating the data table. In other words, they go through locking twice, i.e., [lock, read, unlock] and
     * [lock, write, unlock]. Because of this two phase locking, for a pair of concurrent mutations (for the same row),
     * the same row state can be read from the data table. This means the same existing index row can be made unverified
     * twice with different timestamps, one for each concurrent mutation. These unverified mutations can be repaired
     * from the data table later during HBase scans using the index read repair process. This is one of the reasons
     * for having extra unverified rows in the index table. The other reason is the data table write failures.
     * When a data table write fails, it leaves an unverified index row behind. These rows are never returned to clients,
     * instead they are repaired, which means either they are rebuilt from their data table rows or they are deleted if
     * their data table rows do not exist.
     *
     * Delete Family Version Markers
     *
     * The family version delete markers are generated by the read repair to remove extra unverified rows. They only
     * show up in the actual mutation list since they are not generated for regular table updates or index rebuilds.
     * For the verification purpose, these delete markers can be treated as extra unverified rows and can be safely
     * skipped.
     *
     * Delete Family Markers
     * Delete family markers are generated during read repair, regular table updates and index rebuilds to delete index
     * table rows. The read repair generates them to delete extra unverified rows. During regular table updates or
     * index rebuilds, the delete family markers are used to delete index rows due to data table row deletes or
     * data table row overwrites.
     *
     * Verification Algorithm
     *
     * IndexTool verification generates an expected list of index mutations from the data table rows and uses this list
     * to check if index table rows are consistent with the data table.
     *
     * The expect list is generated using the index rebuild algorithm. This mean for a given row, the list can include
     * a number of put and delete mutations such that the followings hold:
     *
     * Every mutation will include a set of cells with the same timestamp
     * Every mutation has a different timestamp
     * A delete mutation will include only delete family cells and it is for deleting the entire row and its versions
     * Every put mutation is verified
     *
     * For both verification types, after the expected list of index mutations is constructed for a given data table,
     * another list called the actual list of index mutations is constructed by reading the index table row using HBase
     * raw scan and all versions of the cells of the row are retrieved.
     *
     * As in the construction for the expected list, the cells are grouped into a put and a delete set. The put and
     * delete sets for a given row are further grouped based on their timestamps into put and delete mutations such that
     * all the cells in a mutation have the timestamps. The put and delete mutations are then sorted within a single
     * list. Mutations in this list are sorted in ascending order of their timestamp. This list is the actual list.
     *
     * For the without-repair verification, unverified mutations and family version delete markers are removed from
     * the actual list and then the list is compared with the expected list.
     *
     * In case of the with-repair verification, the actual list is first repaired, then unverified mutations and family
     * version delete markers are removed from the actual list and finally the list is compared with the expected list.
     *
     * The actual list is repaired as follows: Every unverified mutation is repaired using the method read repair uses.
     * However, instead of going through actual repair implementation, the expected mutations are used for repair.
     */

    @VisibleForTesting
    public boolean verifySingleIndexRow(byte[] indexRowKey, List<Mutation> actualMutationList, List<Mutation> expectedMutationList,
                                        Set<byte[]> mostRecentIndexRowKeys, List<Mutation> indexRowsToBeDeleted,
                                        IndexToolVerificationResult.PhaseResult verificationPhaseResult,
                                        boolean isBeforeRebuild)
            throws IOException {
        if (expectedMutationList == null) {
            throw new DoNotRetryIOException(NO_EXPECTED_MUTATION);
        }
        if (actualMutationList == null || actualMutationList.isEmpty()) {
            throw new DoNotRetryIOException(ACTUAL_MUTATION_IS_NULL_OR_EMPTY);
        }

        if (isBeforeRebuild) {
            Mutation m = actualMutationList.get(0);
            if (m instanceof Put && (mostRecentIndexRowKeys.isEmpty() || mostRecentIndexRowKeys.contains(m.getRow()))) {
                // We do check here only the latest version as older versions will always be unverified before
                // newer versions are inserted.
                updateUnverifiedIndexRowCounters((Put) m, getTimestamp(expectedMutationList.get(0)), indexRowsToBeDeleted, verificationPhaseResult);
            }
        }
        if (verifyType == IndexTool.IndexVerifyType.ONLY) {
            repairActualMutationList(actualMutationList, expectedMutationList);
        }
        // actualMutationList can be empty after returning from this function
        cleanUpActualMutationList(actualMutationList);

        long currentTime = EnvironmentEdgeManager.currentTimeMillis();
        int actualIndex = 0;
        int expectedIndex = 0;
        int expectedSize = expectedMutationList.size();
        int actualSize = actualMutationList.size();
        Mutation expected = null;
        Mutation previousExpected;
        Mutation actual = null;
        while (expectedIndex < expectedSize && actualIndex <actualSize) {
            previousExpected = expected;
            expected = expectedMutationList.get(expectedIndex);
            // Check if cell expired as per the current server's time and data table ttl
            // Index table should have the same ttl as the data table, hence we might not
            // get a value back from index if it has already expired between our rebuild and
            // verify
            // TODO: have a metric to update for these cases
            if (isTimestampBeforeTTL(indexTableTTL, currentTime, getTimestamp(expected))) {
                verificationPhaseResult.setExpiredIndexRowCount(verificationPhaseResult.getExpiredIndexRowCount() + 1);
                return true;
            }
            actual = actualMutationList.get(actualIndex);
            if (expected instanceof Put) {
                if (previousExpected instanceof Delete) {
                    // Between an expected delete and put, there can be one or more deletes due to
                    // concurrent mutations or data table write failures. Skip all of them if any
                    // There cannot be any actual delete mutation between two expected put mutations.
                    while (getTimestamp(actual) >= getTimestamp(expected) && actual instanceof Delete) {
                        actualIndex++;
                        if (actualIndex == actualSize) {
                            break;
                        }
                        actual = actualMutationList.get(actualIndex);
                    }
                    if (actualIndex == actualSize) {
                        break;
                    }
                }
                if (actual instanceof Delete) {
                    break;
                }
                if (isMatchingMutation(expected, actual)) {
                    expectedIndex++;
                    actualIndex++;
                    continue;
                }
            } else { // expected instanceof Delete
                // Between put and delete, delete and delete, or before the first delete, there can be other deletes.
                // Skip all of them if any
                while (getTimestamp(actual) > getTimestamp(expected) && actual instanceof Delete) {
                    actualIndex++;
                    if (actualIndex == actualSize) {
                        break;
                    }
                    actual = actualMutationList.get(actualIndex);
                }
                if (actualIndex == actualSize) {
                    break;
                }
                if (getTimestamp(actual) == getTimestamp(expected) &&
                        (actual instanceof Delete && isDeleteFamily(actual))) {
                    expectedIndex++;
                    actualIndex++;
                    continue;
                }
            }
            break;
        }

        if (expectedIndex == expectedSize ){
            // every expected mutation has its matching one in the actual list.
            verificationPhaseResult.setValidIndexRowCount(verificationPhaseResult.getValidIndexRowCount() + 1);
            return true;
        }

        if (isTimestampBeyondMaxLookBack(maxLookBackInMills, currentTime, getTimestamp(expectedMutationList.get(expectedIndex)))){
            if (expectedIndex > 0) {
                // if current expected index mutation is beyond max look back window, we only need to make sure its latest
                // mutation is a matching one, as an SCN query is required.
                verificationPhaseResult.
                        setValidIndexRowCount(verificationPhaseResult.getValidIndexRowCount() + 1);
                return true;
            }

            // All expected mutations are beyond the maxLookBack window, none of them can find its matching one in actual list
            // It may be caused by real bug or compaction on the index table.
            // We report it as a failure, so "before" option can trigger the index rebuild for this row.
            // This repair is required, when there is only one index row for a given data table row and the timestamp of that row
            // can be beyond maxLookBack.
            verificationPhaseResult.
                    setBeyondMaxLookBackInvalidIndexRowCount(verificationPhaseResult.getBeyondMaxLookBackInvalidIndexRowCount() + 1);
            byte[] dataKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(indexRowKey), viewConstants);
            String errorMsg = String.format("Expect %1$s mutations but got %2$s (beyond maxLookBack)",
                    expectedSize,
                    actualSize);
            logToIndexToolOutputTable(dataKey, indexRowKey,
                    getTimestamp(expectedMutationList.get(expectedIndex)),
                    0, errorMsg, isBeforeRebuild, BEYOND_MAX_LOOKBACK_INVALID);
            return false;
        }
        else {
            if (actualIndex < actualSize && actual instanceof Put  &&  expected instanceof Put){
                logMismatch(expected, actual, expectedIndex, verificationPhaseResult, isBeforeRebuild);
            }
            else {
                if (expected == null) {
                    // Happens when the actualMutationList becomes empty after returning from
                    // the cleanUpActualMutationList function.
                    expected = expectedMutationList.get(0);
                }
                byte[] dataKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(indexRowKey), viewConstants);
                String errorMsg = String.format("Not matching index row. expectedIndex=%d. expectedMutationSize=%d. actualIndex=%d. actualMutationSize=%d. expectedType=%s. actualType=%s",
                        expectedIndex, expectedSize, actualIndex, actualSize, expected.getClass().getName(), (actualIndex < actualSize ? actual.getClass().getName() : "null"));
                logToIndexToolOutputTable(dataKey, indexRowKey,
                        getTimestamp(expected), (actualIndex < actualSize ? getTimestamp(actual): 0L), errorMsg, isBeforeRebuild,
                        INVALID_ROW);
            }
            verificationPhaseResult.setInvalidIndexRowCount(verificationPhaseResult.getInvalidIndexRowCount() + 1);
            return false;
        }
    }

    protected void verifyIndexRows(Map<byte[], List<Mutation>> actualIndexMutationMap,
                                   Map<byte[], List<Mutation>> expectedIndexMutationMap,
                                   Set<byte[]> mostRecentIndexRowKeys,
                                   List<Mutation> indexRowsToBeDeleted,
                                   IndexToolVerificationResult.PhaseResult verificationPhaseResult,
                                   boolean isBeforeRebuild) throws IOException {
        Map<byte[], List<Mutation>> invalidIndexRows = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        try {
            for (Map.Entry<byte[], List<Mutation>> entry : actualIndexMutationMap.entrySet()) {
                byte[] indexRowKey = entry.getKey();
                List<Mutation> expectedMutationList = expectedIndexMutationMap.get(indexRowKey);
                if (expectedMutationList != null) {
                    if (!verifySingleIndexRow(entry.getKey(), entry.getValue(), expectedMutationList, mostRecentIndexRowKeys,
                            indexRowsToBeDeleted, verificationPhaseResult, isBeforeRebuild)) {
                        invalidIndexRows.put(indexRowKey, expectedMutationList);
                    }
                    expectedIndexMutationMap.remove(indexRowKey);
                } else {
                    logExtraIndexRowAndUpdateCounters(entry.getValue(), verificationPhaseResult, isBeforeRebuild);
                    indexRowsToBeDeleted.add(indexMaintainer.buildRowDeleteMutation(indexRowKey,
                            IndexMaintainer.DeleteType.ALL_VERSIONS, getTimestamp(entry.getValue().get(0))));
                }
            }
        } catch (Throwable t) {
            if (indexHTable != null) {
                ClientUtil.throwIOException(indexHTable.getName().toString(), t);
            } else {
                ClientUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
            }
        }
        List<byte[]> expiredIndexRows = new ArrayList<>();
        // Check if any expected rows from index(which we didn't get) are already expired due to TTL
        // TODO: metrics for expired rows
        long currentTime = EnvironmentEdgeManager.currentTimeMillis();
        for (Map.Entry<byte[], List<Mutation>> entry: expectedIndexMutationMap.entrySet()) {
            List<Mutation> mutationList = entry.getValue();
            if (isTimestampBeforeTTL(indexTableTTL, currentTime, getTimestamp(mutationList.get(mutationList.size() - 1)))) {
                verificationPhaseResult.setExpiredIndexRowCount(verificationPhaseResult.getExpiredIndexRowCount() + 1);
                expiredIndexRows.add(entry.getKey());
            }
        }
        // Remove the expired rows from indexMutationMap
        for (byte[] indexKey : expiredIndexRows) {
            expectedIndexMutationMap.remove(indexKey);
        }
        // Count and log missing rows
        for (Map.Entry<byte[], List<Mutation>> entry: expectedIndexMutationMap.entrySet()) {
            byte[] indexKey = entry.getKey();
            List<Mutation> mutationList = entry.getValue();
            Mutation mutation = mutationList.get(mutationList.size() - 1);
            if (mutation instanceof Delete) {
                continue;
            }
            currentTime = EnvironmentEdgeManager.currentTimeMillis();
            String errorMsg;
            IndexVerificationOutputRepository.IndexVerificationErrorType errorType;
            if (isTimestampBeyondMaxLookBack(maxLookBackInMills, currentTime, getTimestamp(mutation))){
                errorMsg = ERROR_MESSAGE_MISSING_INDEX_ROW_BEYOND_MAX_LOOKBACK;
                errorType = BEYOND_MAX_LOOKBACK_MISSING;
                verificationPhaseResult.
                        setBeyondMaxLookBackMissingIndexRowCount(verificationPhaseResult.getBeyondMaxLookBackMissingIndexRowCount() + 1);
            }
            else {
                errorMsg = ERROR_MESSAGE_MISSING_INDEX_ROW;
                errorType = MISSING_ROW;
                verificationPhaseResult.setMissingIndexRowCount(verificationPhaseResult.getMissingIndexRowCount() + 1);
            }
            byte[] dataKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(indexKey), viewConstants);
            logToIndexToolOutputTable(dataKey, indexKey, getTimestamp(mutation), 0, errorMsg,
                    isBeforeRebuild, errorType);
        }

        // Leave the invalid and missing rows in indexMutationMap
        expectedIndexMutationMap.putAll(invalidIndexRows);
    }

    // After IndexerRegionScanner is removed, this method should become abstract
    protected void commitBatch(List<Mutation> indexUpdates) throws IOException, InterruptedException{
        throw new DoNotRetryIOException("This method has not been implement by the sub class");
    }

    protected void updateIndexRows(Map<byte[], List<Mutation>> indexMutationMap,
                                    List<Mutation> indexRowsToBeDeleted,
                                    IndexToolVerificationResult verificationResult) throws IOException {
        try {
            int batchSize = 0;
            List<Mutation> indexUpdates = new ArrayList<Mutation>(maxBatchSize);
            for (List<Mutation> mutationList : indexMutationMap.values()) {
                indexUpdates.addAll(mutationList);
                batchSize += mutationList.size();
                if (batchSize >= maxBatchSize) {
                    commitBatch(indexUpdates);
                    batchSize = 0;
                    indexUpdates = new ArrayList<Mutation>(maxBatchSize);
                }
            }
            if (batchSize > 0) {
                commitBatch(indexUpdates);
            }
            batchSize = 0;
            indexUpdates = new ArrayList<Mutation>(maxBatchSize);
            for (Mutation mutation : indexRowsToBeDeleted) {
                indexUpdates.add(mutation);
                batchSize ++;
                if (batchSize >= maxBatchSize) {
                    commitBatch(indexUpdates);
                    batchSize = 0;
                    indexUpdates = new ArrayList<Mutation>(maxBatchSize);
                }
            }
            if (batchSize > 0) {
                commitBatch(indexUpdates);
            }
            if (verify) {
                verificationResult.setRebuiltIndexRowCount(verificationResult.getRebuiltIndexRowCount() + indexMutationMap.size());
            }
        } catch (Throwable t) {
            if (indexHTable != null) {
                ClientUtil.throwIOException(indexHTable.getName().toString(), t);
            } else {
                ClientUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
            }
        }
    }

    @VisibleForTesting
    public List<Mutation> prepareActualIndexMutations(Result indexRow) throws IOException {
        Put put = null;
        Delete del = null;
        for (Cell cell : indexRow.rawCells()) {
            if (cell.getType() == Cell.Type.Put) {
                if (put == null) {
                    put = new Put(CellUtil.cloneRow(cell));
                }
                put.add(cell);
            } else {
                if (del == null) {
                    del = new Delete(CellUtil.cloneRow(cell));
                }
                del.add(cell);
            }
        }
        return getMutationsWithSameTS(put, del, MUTATION_TS_DESC_COMPARATOR);
    }

    protected Scan prepareIndexScan(Map<byte[], List<Mutation>> indexMutationMap) throws IOException {
        List<KeyRange> keys = new ArrayList<>(indexMutationMap.size());
        for (byte[] indexKey : indexMutationMap.keySet()) {
            keys.add(PVarbinary.INSTANCE.getKeyRange(indexKey, SortOrder.ASC));
        }

        ScanRanges scanRanges = ScanRanges.createPointLookup(keys);
        Scan indexScan = new Scan();
        indexScan.setTimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMax());
        scanRanges.initializeScan(indexScan);
        SkipScanFilter skipScanFilter = scanRanges.getSkipScanFilter();
        indexScan.setFilter(new SkipScanFilter(skipScanFilter, true, true));
        indexScan.setRaw(true);
        indexScan.readAllVersions();
        indexScan.setCacheBlocks(false);
        return indexScan;
    }

    protected void submitTasks(TaskBatch<Boolean> tasks) throws IOException{
        Pair<List<Boolean>, List<Future<Boolean>>> resultsAndFutures = null;
        try {
            LOGGER.debug("Waiting on index verify and/or rebuild tasks to complete...");
            resultsAndFutures = this.pool.submitUninterruptible(tasks);
        } catch (ExecutionException e) {
            throw new RuntimeException("Should not fail on the results while using a WaitForCompletionTaskRunner", e);
        } catch (EarlyExitFailure e) {
            throw new RuntimeException("Stopped while waiting for batch, quitting!", e);
        }
        int index = 0;
        for (Boolean result : resultsAndFutures.getFirst()) {
            if (result == null) {
                Throwable cause = ServerUtil.getExceptionFromFailedFuture(resultsAndFutures.getSecond().get(index));
                // there was a failure
                throw new IOException(exceptionMessage, cause);
            }
            index++;
        }
    }



    /**
     * This is to reorder the mutations in ascending order by the tuple of timestamp and mutation type where
     * put comes before delete
     */
    public static final Comparator<Mutation> MUTATION_TS_COMPARATOR = new Comparator<Mutation>() {
        @Override
        public int compare(Mutation o1, Mutation o2) {
            long ts1 = getTimestamp(o1);
            long ts2 = getTimestamp(o2);
            if (ts1 < ts2) {
                return -1;
            }
            if (ts1 > ts2) {
                return 1;
            }
            if (o1 instanceof Put && o2 instanceof Delete) {
                return -1;
            }
            if (o1 instanceof Delete && o2 instanceof Put) {
                return 1;
            }
            return 0;
        }
    };

    public static List<Mutation> getMutationsWithSameTS(Put put, Delete del) {
        // Reorder the mutations on the same row so that delete comes before put when they have the same timestamp
        return getMutationsWithSameTS(put, del, MUTATION_TS_COMPARATOR);
    }

    public static List<Mutation> getMutationsWithSameTS(Put put, Delete del, Comparator<Mutation> comparator) {
        List<Mutation> mutationList = Lists.newArrayListWithExpectedSize(2);
        if (put != null) {
            mutationList.add(put);
        }
        if (del != null) {
            mutationList.add(del);
        }
        // Group the cells within a mutation based on their timestamps and create a separate mutation for each group
        mutationList = (List<Mutation>) IndexManagementUtil.flattenMutationsByTimestamp(mutationList);
        // Reorder the mutations
        Collections.sort(mutationList, comparator);
        return mutationList;
    }

    private static Put prepareIndexPutForRebuild(IndexMaintainer indexMaintainer,
            ImmutableBytesPtr rowKeyPtr, ValueGetter mergedRowVG, long ts) throws IOException {
        Put indexPut = indexMaintainer.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
                mergedRowVG, rowKeyPtr, ts, null, null, false);
        if (indexPut == null) {
            // No covered column. Just prepare an index row with the empty column
            byte[] indexRowKey = indexMaintainer.buildRowKey(mergedRowVG, rowKeyPtr,
                    null, null, ts);
            indexPut = new Put(indexRowKey);
        } else {
            IndexUtil.removeEmptyColumn(indexPut, indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                    indexMaintainer.getEmptyKeyValueQualifier());
        }
        indexPut.addColumn(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                indexMaintainer.getEmptyKeyValueQualifier(), ts, VERIFIED_BYTES);
        return indexPut;
    }

    public static void removeColumn(Put put, Cell deleteCell) {
        byte[] family = CellUtil.cloneFamily(deleteCell);
        List<Cell> cellList = put.getFamilyCellMap().get(family);
        if (cellList == null) {
            return;
        }
        Iterator<Cell> cellIterator = cellList.iterator();
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            if (CellUtil.matchingQualifier(cell, deleteCell)) {
                cellIterator.remove();
                if (cellList.isEmpty()) {
                    put.getFamilyCellMap().remove(family);
                }
                return;
            }
        }
    }

    public static void apply(Put destination, Put source) throws IOException {
        for (List<Cell> cells : source.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                if (!destination.has(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell))) {
                    destination.add(cell);
                }
            }
        }
    }

    public static Put applyNew(Put destination, Put source) throws IOException {
        Put next = new Put(destination);
        apply(next, source);
        return next;
    }

    private static void applyDeleteOnPut(Delete del, Put put) throws IOException {
        for (List<Cell> cells : del.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                switch (cell.getType()) {
                    case DeleteFamily:
                        put.getFamilyCellMap().remove(CellUtil.cloneFamily(cell));
                        break;
                    case DeleteColumn:
                        removeColumn(put, cell);
                        break;
                    default:
                        // We do not expect this can happen
                        throw new DoNotRetryIOException("Single version delete marker in data mutation " +
                                del);
                }
            }
        }
    }

    /**
     * Generate the index update for a data row from the mutation that are obtained by merging
     * the previous data row state with the pending row mutation for index rebuild. This method is
     * called only for global indexes including covered full, covered partial, uncovered full, and
     * uncovered partial indexes.
     * pendingMutations is a sorted list of data table mutations that are used to replay index
     * table mutations. This list is sorted in ascending order by the tuple of row key, timestamp
     * and mutation type where delete comes after put.
     */
    public static List<Mutation> prepareIndexMutationsForRebuild(IndexMaintainer indexMaintainer,
                                                                 Put dataPut, Delete dataDel) throws IOException {
        List<Mutation> dataMutations = getMutationsWithSameTS(dataPut, dataDel);
        List<Mutation> indexMutations = Lists.newArrayListWithExpectedSize(dataMutations.size());
        // The row key ptr of the data table row for which we will build index rows here
        ImmutableBytesPtr rowKeyPtr = (dataPut != null) ? new ImmutableBytesPtr(dataPut.getRow()) :
                new ImmutableBytesPtr(dataDel.getRow());
        // Start with empty data table row
        Put currentDataRowState = null;
        // The index row key corresponding to the current data row
        byte[] indexRowKeyForCurrentDataRow = null;
        int dataMutationListSize = dataMutations.size();
        for (int i = 0; i < dataMutationListSize; i++) {
            Mutation mutation = dataMutations.get(i);
            long ts = getTimestamp(mutation);
            Delete deleteToApply = null;
            if (mutation instanceof Put) {
                if (i < dataMutationListSize - 1) {
                    // If there is a pair of a put and delete mutation with the same timestamp then apply the delete
                    // mutation on the put. If the delete mutation deletes all the cells in the put mutation, the family
                    // cell map of the put mutation becomes empty and the mutation is ignored later
                    Mutation nextMutation = dataMutations.get(i + 1);
                    if (getTimestamp(nextMutation) == ts && nextMutation instanceof Delete) {
                        applyDeleteOnPut((Delete) nextMutation, (Put) mutation);
                        if(mutation.getFamilyCellMap().size() != 0) {
                            // Apply the delete mutation on the current data row state too
                            if (currentDataRowState != null) {
                                applyDeleteOnPut((Delete) nextMutation, currentDataRowState);
                                if (currentDataRowState.getFamilyCellMap().size() == 0) {
                                    currentDataRowState = null;
                                    indexRowKeyForCurrentDataRow = null;
                                }
                            }
                        } else {
                            /**
                             * When the mutation is empty after the delete mutation is applied, we should
                             * reuse the logical of applying a delete mutation on currentDataRowState.
                             */
                            deleteToApply = (Delete)nextMutation;
                        }
                        // This increment is to skip the next (delete) mutation as we have already processed it
                        i++;
                    }
                }

                if (mutation.getFamilyCellMap().size() != 0) {
                    // Add this put on top of the current data row state to get the next data row state
                    Put nextDataRow = (currentDataRowState == null) ? new Put((Put) mutation) :
                            applyNew((Put) mutation, currentDataRowState);
                    if (!indexMaintainer.shouldPrepareIndexMutations(nextDataRow)) {
                        currentDataRowState = nextDataRow;
                        if (indexRowKeyForCurrentDataRow != null) {
                            Mutation del = indexMaintainer.buildRowDeleteMutation(
                                    indexRowKeyForCurrentDataRow,
                                    IndexMaintainer.DeleteType.ALL_VERSIONS, ts);
                            indexMutations.add(del);
                        }
                        indexRowKeyForCurrentDataRow = null;
                        continue;
                    }
                    ValueGetter nextDataRowVG = new IndexUtil.SimpleValueGetter(nextDataRow);
                    Put indexPut = prepareIndexPutForRebuild(indexMaintainer, rowKeyPtr,
                            nextDataRowVG, ts);
                    indexMutations.add(indexPut);
                    // Delete the current index row if the new index key is different than the current one
                    if (indexRowKeyForCurrentDataRow != null) {
                        if (Bytes.compareTo(indexPut.getRow(), indexRowKeyForCurrentDataRow) != 0) {
                            Mutation del = indexMaintainer.buildRowDeleteMutation(
                                    indexRowKeyForCurrentDataRow,
                                    IndexMaintainer.DeleteType.ALL_VERSIONS, ts);
                            indexMutations.add(del);
                        }
                    }
                    // For the next iteration of the for loop
                    currentDataRowState = nextDataRow;
                    indexRowKeyForCurrentDataRow = indexPut.getRow();
                    continue;
                }
            } else if(mutation instanceof Delete) {
                deleteToApply = (Delete)mutation;
            }

            if (deleteToApply != null && currentDataRowState != null) {
                // We apply delete mutations only on the current data row state to obtain the next data row state.
                // For the index table, we are only interested in if the index row should be deleted or not.
                // There is no need to apply column deletes to index rows since index rows are always full rows
                // and all the cells in an index row have the same timestamp value. Because of this index rows
                // versions do not share cells.
                applyDeleteOnPut(deleteToApply, currentDataRowState);
                Put nextDataRowState = currentDataRowState;
                if (nextDataRowState.getFamilyCellMap().size() == 0) {
                    if (indexRowKeyForCurrentDataRow != null) {
                        Mutation
                                del =
                                indexMaintainer.buildRowDeleteMutation(indexRowKeyForCurrentDataRow,
                                        IndexMaintainer.DeleteType.ALL_VERSIONS, ts);
                        indexMutations.add(del);
                    }
                    currentDataRowState = null;
                    indexRowKeyForCurrentDataRow = null;
                } else if (indexRowKeyForCurrentDataRow != null) {
                    if (!indexMaintainer.shouldPrepareIndexMutations(nextDataRowState)) {
                        currentDataRowState = nextDataRowState;
                        Mutation del = indexMaintainer.buildRowDeleteMutation(
                                indexRowKeyForCurrentDataRow,
                                IndexMaintainer.DeleteType.ALL_VERSIONS, ts);
                        indexMutations.add(del);
                        indexRowKeyForCurrentDataRow = null;
                        continue;
                    }
                    ValueGetter nextDataRowVG = new IndexUtil.SimpleValueGetter(nextDataRowState);
                    Put indexPut = prepareIndexPutForRebuild(indexMaintainer, rowKeyPtr,
                            nextDataRowVG, ts);
                    indexMutations.add(indexPut);
                    // Delete the current index row if the new index key is different than the current one
                    if (indexRowKeyForCurrentDataRow != null) {
                        if (Bytes.compareTo(indexPut.getRow(), indexRowKeyForCurrentDataRow) != 0) {
                            Mutation del = indexMaintainer.buildRowDeleteMutation(
                                    indexRowKeyForCurrentDataRow,
                                    IndexMaintainer.DeleteType.ALL_VERSIONS, ts);
                            indexMutations.add(del);
                        }
                    }
                    indexRowKeyForCurrentDataRow = indexPut.getRow();
                } else {
                    currentDataRowState = nextDataRowState;
                    indexRowKeyForCurrentDataRow = null;
                }
            }
        }
        return indexMutations;
    }

    @VisibleForTesting
    public int prepareIndexMutations(Put put, Delete del, Map<byte[], List<Mutation>> indexMutationMap,
                                     Set<byte[]> mostRecentIndexRowKeys) throws IOException {
        List<Mutation> indexMutations;

        indexMutations = prepareIndexMutationsForRebuild(indexMaintainer, put, del);
        Collections.reverse(indexMutations);

        boolean mostRecentDone = false;
        // Do not populate mostRecentIndexRowKeys when verifyType is NONE or AFTER
        if (verifyType == IndexTool.IndexVerifyType.NONE || verifyType == IndexTool.IndexVerifyType.AFTER) {
            mostRecentDone = true;
        }
        for (Mutation mutation : indexMutations) {
            byte[] indexRowKey = mutation.getRow();
            List<Mutation> mutationList = indexMutationMap.get(indexRowKey);
            if (mutationList == null) {
                if (!mostRecentDone) {
                    if (mutation instanceof Put) {
                        mostRecentIndexRowKeys.add(indexRowKey);
                        mostRecentDone = true;
                    }
                }
                mutationList = new ArrayList<>();
                mutationList.add(mutation);
                indexMutationMap.put(indexRowKey, mutationList);
            } else {
                mutationList.add(mutation);
            }
        }
        return indexMutations.size();
    }

    static boolean adjustScanFilter(Scan scan) {
        // For rebuilds we use count (*) as query for regular tables which ends up setting the FirstKeyOnlyFilter on scan
        // This filter doesn't give us all columns and skips to the next row as soon as it finds 1 col
        // For rebuilds we need all columns and all versions

        Filter filter = scan.getFilter();
        if (filter instanceof PagingFilter) {
            PagingFilter pageFilter = (PagingFilter) filter;
            Filter delegateFilter = pageFilter.getDelegateFilter();
            if (delegateFilter instanceof EmptyColumnOnlyFilter) {
                pageFilter.setDelegateFilter(null);
            } else if (delegateFilter instanceof FirstKeyOnlyFilter) {
                scan.setFilter(null);
                return true;
            } else if (delegateFilter != null) {
                // Override the filter so that we get all versions
                pageFilter.setDelegateFilter(new AllVersionsIndexRebuildFilter(delegateFilter));
            }
        } else if (filter instanceof EmptyColumnOnlyFilter) {
            scan.setFilter(null);
            return true;
        } else if (filter instanceof FirstKeyOnlyFilter) {
                scan.setFilter(null);
                return true;
        } else if (filter != null) {
            // Override the filter so that we get all versions
            scan.setFilter(new AllVersionsIndexRebuildFilter(filter));
        }
        return false;
    }

    protected RegionScanner getLocalScanner() throws IOException {
        // override the filter to skip scan and open new scanner
        // when lower bound of timerange is passed or newStartKey was populated
        // from previous call to next()
        if(minTimestamp!= 0) {
            Scan incrScan = new Scan(scan);
            incrScan.setTimeRange(minTimestamp, scan.getTimeRange().getMax());
            incrScan.setRaw(true);
            incrScan.readAllVersions();
            incrScan.getFamilyMap().clear();
            incrScan.setCacheBlocks(false);
            for (byte[] family : scan.getFamilyMap().keySet()) {
                incrScan.addFamily(family);
            }
            adjustScanFilter(incrScan);
            if(nextStartKey != null) {
                incrScan.withStartRow(nextStartKey);
            }
            List<KeyRange> keys = new ArrayList<>();

            try (RegionScanner scanner = new TTLRegionScanner(env, incrScan,
                    new PagingRegionScanner(region, region.getScanner(incrScan), incrScan))) {
                List<Cell> row = new ArrayList<>();
                int rowCount = 0;
                // collect row keys that have been modified in the given time-range
                // up to the size of page to build skip scan filter
                do {
                    ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
                    hasMoreIncr = scanner.nextRaw(row);
                    if (!row.isEmpty()) {
                        if (isDummy(row)) {
                            row.clear();
                            continue;
                        }
                        keys.add(PVarbinary.INSTANCE.getKeyRange(CellUtil.cloneRow(row.get(0)),
                            SortOrder.ASC));
                        rowCount++;
                    }
                    row.clear();
                } while (hasMoreIncr && rowCount < pageSizeInRows);
            }
            if (!hasMoreIncr && keys.isEmpty()) {
                return null;
            }
            if (keys.isEmpty()) {
                return innerScanner;
            }
            ScanRanges scanRanges = ScanRanges.createPointLookup(keys);
            scanRanges.initializeScan(incrScan);
            SkipScanFilter skipScanFilter = scanRanges.getSkipScanFilter();
            incrScan.setFilter(new SkipScanFilter(skipScanFilter, true, true));
            //putting back the min time to 0 for index and data reads
            incrScan.setTimeRange(0, scan.getTimeRange().getMax());
            scan.setTimeRange(0, scan.getTimeRange().getMax());
            return region.getScanner(incrScan);
        }
        return innerScanner;
    }

    @Override
    public long getMaxResultSize() {
        return scan.getMaxResultSize();
    }
}

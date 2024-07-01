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
package org.apache.phoenix.coprocessorclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PUnsignedTinyint;

public class BaseScannerRegionObserverConstants {
    public enum ReplayWrite {
        TABLE_AND_INDEX,
        INDEX_ONLY,
        REBUILD_INDEX_ONLY;

        public static ReplayWrite fromBytes(byte[] replayWriteBytes) {
            if (replayWriteBytes == null) {
                return null;
            }
            if (Bytes.compareTo(BaseScannerRegionObserverConstants.REPLAY_TABLE_AND_INDEX_WRITES, replayWriteBytes) == 0) {
                return TABLE_AND_INDEX;
            }
            if (Bytes.compareTo(BaseScannerRegionObserverConstants.REPLAY_ONLY_INDEX_WRITES, replayWriteBytes) == 0) {
                return INDEX_ONLY;
            }
            if (Bytes.compareTo(BaseScannerRegionObserverConstants.REPLAY_INDEX_REBUILD_WRITES, replayWriteBytes) == 0) {
                return REBUILD_INDEX_ONLY;
            }
            throw new IllegalArgumentException("Unknown ReplayWrite code of " + Bytes.toStringBinary(replayWriteBytes));
        }
    }

    public static long getMaxLookbackInMillis(Configuration conf){
        //config param is in seconds, switch to millis
        return conf.getLong(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                BaseScannerRegionObserverConstants.DEFAULT_PHOENIX_MAX_LOOKBACK_AGE) * 1000;
    }

    public static final String AGGREGATORS = "_Aggs";
    public static final String UNORDERED_GROUP_BY_EXPRESSIONS = "_UnorderedGroupByExpressions";
    public static final String KEY_ORDERED_GROUP_BY_EXPRESSIONS = "_OrderedGroupByExpressions";
    public static final String ESTIMATED_DISTINCT_VALUES = "_EstDistinctValues";
    public static final String NON_AGGREGATE_QUERY = "_NonAggregateQuery";
    public static final String TOPN = "_TopN";
    public static final String UNGROUPED_AGG = "_UngroupedAgg";
    public static final String DELETE_AGG = "_DeleteAgg";
    public static final String UPSERT_SELECT_TABLE = "_UpsertSelectTable";
    public static final String UPSERT_SELECT_EXPRS = "_UpsertSelectExprs";
    public static final String DELETE_CQ = "_DeleteCQ";
    public static final String DELETE_CF = "_DeleteCF";
    public static final String UPSERT_STATUS_CQ = "_UpsertStatusCQ";
    public static final String UPSERT_CF = "_UpsertCF";
    public static final String EMPTY_CF = "_EmptyCF";
    public static final String EMPTY_COLUMN_QUALIFIER = "_EmptyColumnQualifier";
    public static final String SPECIFIC_ARRAY_INDEX = "_SpecificArrayIndex";
    public static final String GROUP_BY_LIMIT = "_GroupByLimit";
    public static final String LOCAL_INDEX = "_LocalIndex";
    public static final String LOCAL_INDEX_BUILD = "_LocalIndexBuild";
    public static final String UNCOVERED_GLOBAL_INDEX = "_UncoveredGlobalIndex";
    public static final String INDEX_REBUILD_PAGING = "_IndexRebuildPaging";
    // The number of index rows to be rebuild in one RPC call
    public static final String INDEX_REBUILD_PAGE_ROWS = "_IndexRebuildPageRows";
    public static final String INDEX_PAGE_ROWS = "_IndexPageRows";
    public static final String SERVER_PAGE_SIZE_MS = "_ServerPageSizeMs";
    // Index verification type done by the index tool
    public static final String INDEX_REBUILD_VERIFY_TYPE = "_IndexRebuildVerifyType";
    public static final String INDEX_RETRY_VERIFY = "_IndexRetryVerify";
    public static final String INDEX_REBUILD_DISABLE_LOGGING_VERIFY_TYPE =
            "_IndexRebuildDisableLoggingVerifyType";
    public static final String INDEX_REBUILD_DISABLE_LOGGING_BEYOND_MAXLOOKBACK_AGE =
            "_IndexRebuildDisableLoggingBeyondMaxLookbackAge";
    @Deprecated
    public static final String LOCAL_INDEX_FILTER = "_LocalIndexFilter";
    @Deprecated
    public static final String LOCAL_INDEX_LIMIT = "_LocalIndexLimit";
    @Deprecated
    public static final String LOCAL_INDEX_FILTER_STR = "_LocalIndexFilterStr";
    public static final String INDEX_FILTER = "_IndexFilter";
    public static final String INDEX_LIMIT = "_IndexLimit";
    public static final String INDEX_FILTER_STR = "_IndexFilterStr";

    /*
     * Attribute to denote that the index maintainer has been serialized using its proto-buf presentation.
     * Needed for backward compatibility purposes. TODO: get rid of this in next major release.
     */
    public static final String LOCAL_INDEX_BUILD_PROTO = "_LocalIndexBuild";
    public static final String LOCAL_INDEX_JOIN_SCHEMA = "_LocalIndexJoinSchema";
    public static final String DATA_TABLE_COLUMNS_TO_JOIN = "_DataTableColumnsToJoin";
    public static final String COLUMNS_STORED_IN_SINGLE_CELL = "_ColumnsStoredInSingleCell";
    public static final String VIEW_CONSTANTS = "_ViewConstants";
    public static final String EXPECTED_UPPER_REGION_KEY = "_ExpectedUpperRegionKey";
    public static final String REVERSE_SCAN = "_ReverseScan";
    public static final String ANALYZE_TABLE = "_ANALYZETABLE";
    public static final String REBUILD_INDEXES = "_RebuildIndexes";
    public static final String DO_TRANSFORMING = "_DoTransforming";
    public static final String TX_STATE = "_TxState";
    public static final String GUIDEPOST_WIDTH_BYTES = "_GUIDEPOST_WIDTH_BYTES";
    public static final String GUIDEPOST_PER_REGION = "_GUIDEPOST_PER_REGION";
    public static final String UPGRADE_DESC_ROW_KEY = "_UPGRADE_DESC_ROW_KEY";
    public static final String SCAN_REGION_SERVER = "_SCAN_REGION_SERVER";
    public static final String RUN_UPDATE_STATS_ASYNC_ATTRIB = "_RunUpdateStatsAsync";
    public static final String SKIP_REGION_BOUNDARY_CHECK = "_SKIP_REGION_BOUNDARY_CHECK";
    public static final String TX_SCN = "_TxScn";
    public static final String PHOENIX_TTL = "_PhoenixTTL";
    public static final String MASK_PHOENIX_TTL_EXPIRED = "_MASK_TTL_EXPIRED";
    public static final String DELETE_PHOENIX_TTL_EXPIRED = "_DELETE_TTL_EXPIRED";
    public static final String PHOENIX_TTL_SCAN_TABLE_NAME = "_PhoenixTTLScanTableName";
    public static final String SCAN_ACTUAL_START_ROW = "_ScanActualStartRow";
    public static final String REPLAY_WRITES = "_IGNORE_NEWER_MUTATIONS";
    public final static String SCAN_OFFSET = "_RowOffset";
    public static final String SCAN_START_ROW_SUFFIX = "_ScanStartRowSuffix";
    public static final String SCAN_STOP_ROW_SUFFIX = "_ScanStopRowSuffix";
    public final static String MIN_QUALIFIER = "_MinQualifier";
    public final static String MAX_QUALIFIER = "_MaxQualifier";
    public final static String USE_NEW_VALUE_COLUMN_QUALIFIER = "_UseNewValueColumnQualifier";
    public final static String QUALIFIER_ENCODING_SCHEME = "_QualifierEncodingScheme";
    public final static String IMMUTABLE_STORAGE_ENCODING_SCHEME = "_ImmutableStorageEncodingScheme";
    public final static String USE_ENCODED_COLUMN_QUALIFIER_LIST = "_UseEncodedColumnQualifierList";
    public static final String CLIENT_VERSION = "_ClientVersion";
    public static final String CHECK_VERIFY_COLUMN = "_CheckVerifyColumn";
    public static final String PHYSICAL_DATA_TABLE_NAME = "_PhysicalDataTableName";
    public static final String EMPTY_COLUMN_FAMILY_NAME = "_EmptyCFName";
    public static final String EMPTY_COLUMN_QUALIFIER_NAME = "_EmptyCQName";
    public static final String INDEX_ROW_KEY = "_IndexRowKey";
    public static final String READ_REPAIR_TRANSFORMING_TABLE = "_ReadRepairTransformingTable";

    /**
     * The scan attribute to provide the scan start rowkey for analyze table queries.
     */
    public static final String SCAN_ANALYZE_ACTUAL_START_ROW = "_ScanAnalyzeActualStartRow";

    /**
     * The scan attribute to provide the scan stop rowkey for analyze table queries.
     */
    public static final String SCAN_ANALYZE_ACTUAL_STOP_ROW = "_ScanAnalyzeActualStopRow";

    /**
     * The scan attribute to provide the scan start rowkey include boolean value for analyze table
     * queries.
     */
    public static final String SCAN_ANALYZE_INCLUDE_START_ROW = "_ScanAnalyzeIncludeStartRow";

    /**
     * The scan attribute to provide the scan stop rowkey include boolean value for analyze table
     * queries.
     */
    public static final String SCAN_ANALYZE_INCLUDE_STOP_ROW = "_ScanAnalyzeIncludeStopRow";

    /**
     * The scan attribute to determine whether client changes are compatible to consume
     * new format changes sent by the server. This attribute is mainly used to address
     * data integrity issues related to region moves (PHOENIX-7106).
     */
    public static final String SCAN_SERVER_RETURN_VALID_ROW_KEY = "_ScanServerValidRowKey";


    public final static byte[] REPLAY_TABLE_AND_INDEX_WRITES = PUnsignedTinyint.INSTANCE.toBytes(1);
    public final static byte[] REPLAY_ONLY_INDEX_WRITES = PUnsignedTinyint.INSTANCE.toBytes(2);
    // In case of Index Write failure, we need to determine that Index mutation
    // is part of normal client write or Index Rebuilder. # PHOENIX-5080
    public final static byte[] REPLAY_INDEX_REBUILD_WRITES = PUnsignedTinyint.INSTANCE.toBytes(3);

    public static final String PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY =
            "phoenix.max.lookback.age.seconds";
    public static final int DEFAULT_PHOENIX_MAX_LOOKBACK_AGE = 0;

    /**
     * Attribute name used to pass custom annotations in Scans and Mutations (later). Custom annotations
     * are used to augment log lines emitted by Phoenix. See https://issues.apache.org/jira/browse/PHOENIX-1198.
     */
    public static final String CUSTOM_ANNOTATIONS = "_Annot";

    /** Exposed for testing */
    public static final String SCANNER_OPENED_TRACE_INFO = "Scanner opened on server";
}
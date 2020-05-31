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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfoUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.parallel.TaskBatch;
import org.apache.phoenix.hbase.index.parallel.TaskRunner;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolBuilder;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolManager;
import org.apache.phoenix.hbase.index.parallel.WaitForCompletionTaskRunner;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import static org.apache.phoenix.hbase.index.write.AbstractParallelWriterIndexCommitter.INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY;
import static org.apache.phoenix.query.QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB;

public abstract class GlobalIndexRegionScanner extends BaseRegionScanner {

    public static final String NUM_CONCURRENT_INDEX_VERIFY_THREADS_CONF_KEY = "index.verify.threads.max";
    public static final int DEFAULT_CONCURRENT_INDEX_VERIFY_THREADS = 16;
    public static final String INDEX_VERIFY_ROW_COUNTS_PER_TASK_CONF_KEY = "index.verify.row.count.per.task";
    public static final int DEFAULT_INDEX_VERIFY_ROW_COUNTS_PER_TASK = 2048;
    public static final String NO_EXPECTED_MUTATION = "No expected mutation";
    public static final String ACTUAL_MUTATION_IS_NULL_OR_EMPTY = "actualMutationList is null or empty";
    public static final String ERROR_MESSAGE_MISSING_INDEX_ROW_BEYOND_MAX_LOOKBACK = "Missing index row beyond maxLookBack";
    public static final String ERROR_MESSAGE_MISSING_INDEX_ROW = "Missing index row";

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
    protected Table indexHTable;
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

    public GlobalIndexRegionScanner(RegionScanner innerScanner, final Region region, final Scan scan,
                                    final RegionCoprocessorEnvironment env) throws IOException {
        super(innerScanner);
        final Configuration config = env.getConfiguration();
        if (scan.getAttribute(BaseScannerRegionObserver.INDEX_REBUILD_PAGING) != null) {
            pageSizeInRows = config.getLong(INDEX_REBUILD_PAGE_SIZE_IN_ROWS,
                    QueryServicesOptions.DEFAULT_INDEX_REBUILD_PAGE_SIZE_IN_ROWS);
        }
        maxBatchSize = config.getInt(MUTATE_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
        maxBatchSizeBytes = config.getLong(MUTATE_BATCH_SIZE_BYTES_ATTRIB,
                QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE_BYTES);
        blockingMemstoreSize = UngroupedAggregateRegionObserver.getBlockingMemstoreSize(region, config);
        clientVersionBytes = scan.getAttribute(BaseScannerRegionObserver.CLIENT_VERSION);
        familyMap = scan.getFamilyMap();
        if (familyMap.isEmpty()) {
            familyMap = null;
        }
        indexMetaData = scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD);
        if (indexMetaData == null) {
            indexMetaData = scan.getAttribute(PhoenixIndexCodec.INDEX_MD);
        }
        List<IndexMaintainer> maintainers = IndexMaintainer.deserialize(indexMetaData, true);
        indexMaintainer = maintainers.get(0);
        this.scan = scan;
        this.innerScanner = innerScanner;
        this.region = region;
        byte[] valueBytes = scan.getAttribute(BaseScannerRegionObserver.INDEX_REBUILD_VERIFY_TYPE);
        if (valueBytes != null) {
            verifyType = IndexTool.IndexVerifyType.fromValue(valueBytes);
            if (verifyType != IndexTool.IndexVerifyType.NONE) {
                verify = true;
            }
        }
        // Create the following objects only for rebuilds by IndexTool
        hTableFactory = ServerUtil.getDelegateHTableFactory(env, ServerUtil.ConnectionType.INDEX_WRITER_CONNECTION);
        indexHTable = hTableFactory.getTable(new ImmutableBytesPtr(indexMaintainer.getIndexTableName()));
        indexTableTTL = indexHTable.getTableDescriptor().getColumnFamilies()[0].getTimeToLive();
        maxLookBackInMills = ScanInfoUtil.getMaxLookbackInMillis(config);
        pool = new WaitForCompletionTaskRunner(ThreadPoolManager.getExecutor(
                new ThreadPoolBuilder("IndexVerify",
                        env.getConfiguration()).setMaxThread(NUM_CONCURRENT_INDEX_VERIFY_THREADS_CONF_KEY,
                        DEFAULT_CONCURRENT_INDEX_VERIFY_THREADS).setCoreTimeout(
                        INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY), env));
        rowCountPerTask = config.getInt(INDEX_VERIFY_ROW_COUNTS_PER_TASK_CONF_KEY,
                DEFAULT_INDEX_VERIFY_ROW_COUNTS_PER_TASK);
    }

    public static class SimpleValueGetter implements ValueGetter {
        final ImmutableBytesWritable valuePtr = new ImmutableBytesWritable();
        final Put put;
        public SimpleValueGetter (final Put put) {
            this.put = put;
        }
        @Override
        public ImmutableBytesWritable getLatestValue(ColumnReference ref, long ts) {
            List<Cell> cellList = put.get(ref.getFamily(), ref.getQualifier());
            if (cellList == null || cellList.isEmpty()) {
                return null;
            }
            Cell cell = cellList.get(0);
            valuePtr.set(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            return valuePtr;
        }

        @Override
        public byte[] getRowKey() {
            return put.getRow();
        }

    }

    public static byte[] getIndexRowKey(IndexMaintainer indexMaintainer, final Put dataRow) {
        ValueGetter valueGetter = new SimpleValueGetter(dataRow);
        return indexMaintainer.buildRowKey(valueGetter, new ImmutableBytesWritable(dataRow.getRow()),
                null, null, HConstants.LATEST_TIMESTAMP);
    }

    public static long getMaxTimestamp(Mutation m) {
        long ts = 0;
        for (List<Cell> cells : m.getFamilyCellMap().values()) {
            if (cells == null) {
                continue;
            }
            for (Cell cell : cells) {
                if (ts < cell.getTimestamp()) {
                    ts = cell.getTimestamp();
                }
            }
        }
        return ts;
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
        if (!ScanInfoUtil.isMaxLookbackTimeEnabled(maxLookBackInMills)) {
            return false;
        }
        return tsToCheck < (currentTime - maxLookBackInMills);
    }

    protected static long getMaxTimestamp(Pair<Put, Delete> pair) {
        Put put = pair.getFirst();
        long ts1 = 0;
        if (put != null) {
            ts1 = getMaxTimestamp(put);
        }
        Delete del = pair.getSecond();
        long ts2 = 0;
        if (del != null) {
            ts1 = getMaxTimestamp(del);
        }
        return (ts1 > ts2) ? ts1 : ts2;
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
}

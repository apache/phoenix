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
package org.apache.phoenix.index;

import static org.apache.phoenix.index.IndexMaintainer.getIndexMaintainer;
import static org.apache.phoenix.query.QueryConstants.UNVERIFIED_BYTES;
import static org.apache.phoenix.query.QueryConstants.VERIFIED_BYTES;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;
import static org.apache.phoenix.util.ScanUtil.getDummyResult;
import static org.apache.phoenix.util.ScanUtil.getPageSizeMsForRegionScanner;
import static org.apache.phoenix.util.ScanUtil.isDummy;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.PackagePrivateFieldAccessor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.PhoenixScannerContext;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compat.hbase.CompatScanMetrics;
import org.apache.phoenix.coprocessor.BaseRegionScanner;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.DataTableScanMetrics;
import org.apache.phoenix.coprocessor.DelegateRegionScanner;
import org.apache.phoenix.coprocessor.UncoveredGlobalIndexRegionScanner;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.filter.DistinctPrefixFilter;
import org.apache.phoenix.filter.PagingFilter;
import org.apache.phoenix.filter.UnverifiedRowFilter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.metrics.GlobalIndexCheckerSource;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.transform.TransformMaintainer;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;

/**
 * Coprocessor that verifies the scanned rows of a non-transactional global index. If an index row
 * is unverified (i.e., the row status is unverified), the following steps are taken : (1) We
 * generate the data row key from the index row key, and check if the data row exists. If not, this
 * unverified index row is skipped (i.e., not returned to the client), and it is deleted if it is
 * old enough. The age check is necessary in order not to delete the index rows that are currently
 * being updated. If the data row exists, we continue with the rest of the steps. (2) The index row
 * is rebuilt from the data row. (3) The current scanner is closed as the newly rebuilt row will not
 * be visible to the current scanner. (4) if the data row does not point back to the unverified
 * index row (i.e., the index row key generated from the data row does not match with the row key of
 * the unverified index row), this unverified row is skipped and and it is deleted if it is old
 * enough. A new scanner is opened starting form the index row after this unverified index row. (5)
 * if the data points back to the unverified index row then, a new scanner is opened starting form
 * the index row. The next row is scanned to check if it is verified. if it is verified, it is
 * returned to the client. If not, then it means the data table row timestamp is lower than than the
 * timestamp of the unverified index row, and the index row that has been rebuilt from the data
 * table row is masked by this unverified row. This happens if the first phase updates (i.e.,
 * unverified index row updates) complete but the second phase updates (i.e., data table row
 * updates) fail. There could be back to back such events so we need to scan older versions to
 * retrieve the verified version that is masked by the unverified version(s).
 */
public class GlobalIndexChecker extends BaseScannerRegionObserver implements RegionCoprocessor {
  private static final Logger LOG = LoggerFactory.getLogger(GlobalIndexChecker.class);
  private static final String REPAIR_LOGGING_PERCENT_ATTRIB =
    "phoenix.index.repair.logging.percent";
  private static final double DEFAULT_REPAIR_LOGGING_PERCENT = 100;
  /**
   * Server-only scan attribute that holds the row limit of the client page filter. The checker sets
   * this attribute in preScannerOpen and removes any value that the client supplies. Only the
   * scanner hook of the checker reads this attribute.
   */
  private static final String PAGE_SIZE_ATTRIB = "_GlobalIndexCheckerPageSize";

  private GlobalIndexCheckerSource metricsSource;
  private CoprocessorEnvironment env;

  public enum RebuildReturnCode {
    NO_DATA_ROW(0),
    NO_INDEX_ROW(1),
    INDEX_ROW_EXISTS(2);

    private int value;

    RebuildReturnCode(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  /**
   * Replaces each distinct prefix filter in a copy of the client filter with the filter of the main
   * scan. The distinct prefix filter keeps the row key prefix of the last row that the scan
   * returns. All scanners of the scan must share this state. Otherwise the scan can return two rows
   * with the same prefix. The method builds new filter lists and keeps the direction of each list.
   */
  @VisibleForTesting
  static Filter shareDistinctPrefixFilter(Filter filter,
    DistinctPrefixFilter distinctPrefixFilter) {
    if (distinctPrefixFilter == null) {
      return filter;
    }
    if (filter instanceof DistinctPrefixFilter) {
      return distinctPrefixFilter;
    }
    if (filter instanceof FilterList) {
      FilterList filterList = (FilterList) filter;
      List<Filter> filters = new ArrayList<>(filterList.getFilters().size());
      for (Filter child : filterList.getFilters()) {
        filters.add(shareDistinctPrefixFilter(child, distinctPrefixFilter));
      }
      FilterList newFilterList = new FilterList(filterList.getOperator(), filters);
      // The constructor does not set the direction of the list itself. Copy it from the old list.
      newFilterList.setReversed(filterList.isReversed());
      return newFilterList;
    }
    return filter;
  }

  /**
   * Class that verifies a given row of a non-transactional global index. An instance of this class
   * is created for each scanner on an index and used to verify individual rows and rebuild them if
   * they are not valid
   */
  public class GlobalIndexScanner extends BaseRegionScanner {
    private RegionScanner scanner;
    private long ageThreshold;
    private Scan scan;
    private Scan indexScan;
    private Scan singleRowIndexScan;
    private Scan buildIndexScanForDataTable = null;
    private Table dataHTable = null;
    private byte[] emptyCF;
    private byte[] emptyCQ;
    private IndexMaintainer indexMaintainer = null;
    private byte[][] viewConstants = null;
    private RegionCoprocessorEnvironment env;
    private Region region;
    private long minTimestamp;
    private long maxTimestamp;
    private GlobalIndexCheckerSource metricsSource;
    private long rowCount = 0;
    private long pageSize = Long.MAX_VALUE;
    private boolean hasMore;
    private double loggingPercent;
    private Random random;
    private String indexName;
    private long pageSizeMs;
    private boolean initialized = false;
    private boolean isScanMetricsEnabled = false;
    // The paging filter in the filter chain of the current scanner
    private PagingFilter pagingFilter;
    private final StreamReadSwitch streamReadSwitch;
    // Client filters can keep state. For example, a skip scan filter keeps seek hints. A reopened
    // scanner starts at or after the last row that the current scanner reads. Thus it shares the
    // filter chain of the current scanner. A stream read scanner can start behind the state of
    // that chain, and a repair scan reads one row again. Thus each of these scanners gets a new
    // chain. The state of a distinct prefix filter applies to the full scan, so each new chain
    // shares that filter.
    private FilterProtos.Filter clientFilterProto = null;
    private DistinctPrefixFilter distinctPrefixFilter = null;

    /**
     * Creates a scanner for a scan that the checker did not prepare in preScannerOpen. The scanner
     * gets the row limit from the page filter of the scan, if the scan has one.
     */
    public GlobalIndexScanner(RegionCoprocessorEnvironment env, Scan scan, RegionScanner scanner,
      GlobalIndexCheckerSource metricsSource) throws IOException {
      this(env, scan, scanner, metricsSource, Long.MAX_VALUE);
    }

    /**
     * Creates a scanner with the row limit that preScannerOpen removed from the scan.
     */
    GlobalIndexScanner(RegionCoprocessorEnvironment env, Scan scan, RegionScanner scanner,
      GlobalIndexCheckerSource metricsSource, long pageSize) throws IOException {
      super(scanner);
      this.env = env;
      this.scan = scan;
      this.scanner = scanner;
      this.metricsSource = metricsSource;

      region = env.getRegion();
      emptyCF = scan.getAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_FAMILY_NAME);
      emptyCQ = scan.getAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_QUALIFIER_NAME);
      ageThreshold = env.getConfiguration().getLong(
        QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB,
        QueryServicesOptions.DEFAULT_GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS);
      minTimestamp = scan.getTimeRange().getMin();
      maxTimestamp = scan.getTimeRange().getMax();
      byte[] indexTableNameBytes = region.getRegionInfo().getTable().getName();
      this.indexName = Bytes.toString(indexTableNameBytes);
      indexMaintainer = deserializeIndexMaintainer(region, scan);
      this.pageSize = pageSize;
      loggingPercent = env.getConfiguration().getDouble(REPAIR_LOGGING_PERCENT_ATTRIB,
        DEFAULT_REPAIR_LOGGING_PERCENT);
      random = new Random(EnvironmentEdgeManager.currentTimeMillis());
      pageSizeMs = getPageSizeMsForRegionScanner(scan);
      isScanMetricsEnabled =
        scan.isScanMetricsEnabled() && CompatScanMetrics.supportsFineGrainedReadMetrics();
      pagingFilter = ScanUtil.getPhoenixPagingFilter(scan);
      streamReadSwitch = new StreamReadSwitch(scan);
    }

    @Override
    public int getBatch() {
      return scanner.getBatch();
    }

    @Override
    public long getMaxResultSize() {
      return scanner.getMaxResultSize();
    }

    /**
     * Applies the scan changes that preScannerOpen did not apply. The checker usually changes the
     * scan before HBase opens the region scanner, so this method has no effect. A scanner that
     * another observer creates directly can receive a scan that the checker did not change. Then
     * this method changes the scan and replaces the region scanner. HBase does not ship the
     * replacement scanner, so the stream read switch applies to it.
     */
    private void init() throws IOException {
      boolean scanChanged = false;
      PageFilter pageFilter = ScanUtil.removePageFilter(scan);
      if (pageFilter != null) {
        pageSize = pageFilter.getPageSize();
        scanChanged = true;
      }
      if (addUnverifiedRowFilter(scan, indexMaintainer)) {
        scanChanged = true;
      }
      if (scanChanged) {
        scanner.close();
        scanner = openScanner(scan);
      }
    }

    /**
     * Opens a scanner on the index region and records it as a scanner that the checker owns.
     */
    private RegionScanner openScanner(Scan newScan) throws IOException {
      RegionScanner newScanner = ((DelegateRegionScanner) delegate).getNewRegionScanner(newScan);
      streamReadSwitch.scannerOpened(newScan);
      return newScanner;
    }

    /**
     * Gets the scan for the scanners that the checker opens to continue the index scan. Read repair
     * writes index rows that are newer than the read point of the original scan. The scan has no
     * read point, so that each new scanner can see the repaired rows.
     */
    private Scan getIndexScan() throws IOException {
      if (indexScan == null) {
        indexScan = new Scan(scan);
        PackagePrivateFieldAccessor.setMvccReadPoint(indexScan, -1);
      }
      return indexScan;
    }

    /**
     * Gives the scan a new filter chain. A filter object that two scanners share gets state from
     * both scanners. For example, a skip scan filter can then issue a seek hint behind its previous
     * hint. The new chain is a copy of the client part of the main chain. The only exception is the
     * distinct prefix filter, which the copy shares with the main scan. A repair scan of one row
     * gets no paging filter. A paging filter stops a scan only between rows, so it cannot limit a
     * scan of one row.
     */
    private void setNewFilter(Scan newScan, boolean withPagingFilter) throws IOException {
      if (clientFilterProto == null) {
        Filter clientFilter = getDelegateFilter(scan);
        if (clientFilter instanceof UnverifiedRowFilter) {
          clientFilter = ((UnverifiedRowFilter) clientFilter).getDelegateFilter();
        }
        if (clientFilter != null) {
          // The serialized form holds only the filter configuration, not the scan position. Thus
          // one serialized form serves all new chains. Repair and the stream read switch are rare,
          // so one parse for each new chain is acceptable.
          clientFilterProto = ProtobufUtil.toFilter(clientFilter);
          distinctPrefixFilter = ScanUtil.findDistinctPrefixFilter(clientFilter);
        }
      }
      Filter filter = null;
      if (clientFilterProto != null) {
        // The serialized form does not hold the scan direction. Thus the copy gets the direction
        // of the main scan. The shared distinct prefix filter already has this direction. All
        // filters in a filter list must have the same direction.
        filter = ProtobufUtil.toFilter(clientFilterProto);
        filter.setReversed(scan.isReversed());
        filter = shareDistinctPrefixFilter(filter, distinctPrefixFilter);
      }
      if (withPagingFilter && pagingFilter != null) {
        filter = new PagingFilter(filter, ScanUtil.getPageSizeMsForFilter(scan));
      }
      newScan.setFilter(filter);
      addUnverifiedRowFilter(newScan, indexMaintainer);
    }

    /**
     * Opens a new scanner for the index scan at the given row. The caller closes the current
     * scanner first. The new scanner uses the read type of the scan, so it starts with pread again.
     * It can switch to stream read on its own, like each new scanner in HBase.
     */
    private void reopenScanner(byte[] startRow, boolean inclusive) throws IOException {
      Scan newScan = getIndexScan();
      newScan.withStartRow(startRow, inclusive);
      scanner = openScanner(newScan);
    }

    /**
     * Replaces the current scanner with a stream read scanner. The new scanner starts after the
     * last row that the current scanner returned. If the current scanner returned no row, the new
     * scanner starts at the start row of the current scan. The new scanner uses the same read
     * point. Thus it returns the rows that the current scanner did not return yet.
     */
    private void switchToStreamRead() throws IOException {
      // The current scanner can read past the restart row, so the current filter chain can be
      // past rows that the new scanner reads again. Thus the new scanner gets a new chain. The
      // index scan keeps this chain, so a later reopened scanner shares it.
      setNewFilter(getIndexScan(), true);
      pagingFilter = ScanUtil.getPhoenixPagingFilter(getIndexScan());
      Scan streamScan =
        streamReadSwitch.createStreamScan(getIndexScan(), scanner.getMvccReadPoint());
      // The old scanner stays open until the new scanner holds the read point. Thus a compaction
      // cannot remove cell versions that the new scanner must read.
      RegionScanner oldScanner = scanner;
      try {
        scanner = openScanner(streamScan);
      } finally {
        oldScanner.close();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Index scan on region {} switches to stream read at row {}, inclusive {}",
          region.getRegionInfo().getRegionNameAsString(),
          Bytes.toStringBinary(streamScan.getStartRow()), streamScan.includeStartRow());
      }
    }

    public boolean next(List<Cell> result, boolean raw, ScannerContext scannerContext)
      throws IOException {
      try {
        if (!initialized) {
          init();
          initialized = true;
        }
        do {
          if (streamReadSwitch.isPending()) {
            switchToStreamRead();
          }
          boolean wasReturnImmediately = PhoenixScannerContext.isReturnImmediately(scannerContext);
          if (raw) {
            hasMore = (scannerContext == null)
              ? scanner.nextRaw(result)
              : scanner.nextRaw(result, scannerContext);
          } else {
            hasMore = (scannerContext == null)
              ? scanner.next(result)
              : scanner.next(result, scannerContext);
          }
          streamReadSwitch.resultReturned(result);
          streamReadSwitch.checkPreadLimit(wasReturnImmediately, scannerContext, pageSizeMs,
            pagingFilter);
          if (result.isEmpty()) {
            return hasMore;
          }
          if (isDummy(result)) {
            return true;
          }
          Cell cell = result.get(0);
          if (verifyRowAndRepairIfNecessary(result)) {
            // A read repair can end with a dummy row. A dummy row marks a scan position and does
            // not count against the row limit.
            if (isDummy(result)) {
              return true;
            }
            break;
          }
          if (
            hasMore && (PhoenixScannerContext.isTimedOut(scannerContext, pageSizeMs)
              || PhoenixScannerContext.isReturnImmediately(scannerContext))
          ) {
            byte[] rowKey = CellUtil.cloneRow(cell);
            result.clear();
            getDummyResult(rowKey, result);
            return true;
          }
          // skip this row as it is invalid
          // if there is no more row, then result will be an empty list
        } while (hasMore);
        rowCount++;
        if (rowCount == pageSize) {
          return false;
        }
        return hasMore;
      } catch (Throwable t) {
        ClientUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
        return false; // impossible
      }
    }

    @Override
    public boolean next(List<Cell> result) throws IOException {
      return next(result, false, null);
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
      return next(result, true, null);
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
      return next(result, false, scannerContext);
    }

    @Override
    public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
      return next(result, true, scannerContext);
    }

    @Override
    public void close() throws IOException {
      scanner.close();
      if (dataHTable != null) {
        dataHTable.close();
      }
    }

    @Override
    public RegionInfo getRegionInfo() {
      return scanner.getRegionInfo();
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
      return scanner.reseek(row);
    }

    @Override
    public long getMvccReadPoint() {
      return scanner.getMvccReadPoint();
    }

    private DataTableScanMetrics buildDataTableScanMetrics(ScanMetrics scanMetrics) {
      DataTableScanMetrics.Builder builder = new DataTableScanMetrics.Builder();
      DataTableScanMetrics.populateDataTableScanMetrics(scanMetrics, builder);
      return builder.build();
    }

    private void repairIndexRows(byte[] indexRowKey, long ts, List<Cell> row) throws IOException {
      if (buildIndexScanForDataTable == null) {
        buildIndexScanForDataTable = new Scan();
        getIndexScan();
        singleRowIndexScan = new Scan(scan);
        // Scanners to be opened on index table using indexScan and singleRowIndexScan do
        // require scanning the latest/newer version of cells i.e. new rows updated on the
        // index table as part of the read-repair operation.
        // Both scan objects copy mvcc read point from the original scan object used to
        // open GlobalIndexScanner. Hence, we need to reset them to -1 so that if the
        // region moves in the middle of the ongoing scan on the data table, the reset
        // mvcc value of -1 will ensure that new scanners opened on index table using
        // indexScan and singleRowIndexScan are able to read the latest snapshot of the
        // index updates.
        PackagePrivateFieldAccessor.setMvccReadPoint(singleRowIndexScan, -1);
        byte[] dataTableName =
          scan.getAttribute(BaseScannerRegionObserverConstants.PHYSICAL_DATA_TABLE_NAME);
        dataHTable = ServerUtil.ConnectionFactory
          .getConnection(ServerUtil.ConnectionType.INDEX_WRITER_CONNECTION, env)
          .getTable(TableName.valueOf(dataTableName));

        viewConstants = IndexUtil.deserializeViewConstantsFromScan(scan);
        // The following attributes are set to instruct UngroupedAggregateRegionObserver to do
        // partial index rebuild
        // i.e., rebuild a subset of index rows.
        buildIndexScanForDataTable.setAttribute(BaseScannerRegionObserverConstants.UNGROUPED_AGG,
          TRUE_BYTES);
        buildIndexScanForDataTable.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD,
          scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD));
        ScanUtil.annotateScanWithMetadataAttributes(scan, buildIndexScanForDataTable);
        buildIndexScanForDataTable.setAttribute(BaseScannerRegionObserverConstants.REBUILD_INDEXES,
          TRUE_BYTES);
        buildIndexScanForDataTable.setAttribute(
          BaseScannerRegionObserverConstants.SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));
        buildIndexScanForDataTable
          .setAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_QUALIFIER_NAME, emptyCQ);
        // Scan only columns included in the index table plus the empty column
        for (ColumnReference column : indexMaintainer.getAllColumnsForDataTable()) {
          buildIndexScanForDataTable.addColumn(column.getFamily(), column.getQualifier());
        }
        buildIndexScanForDataTable.addColumn(indexMaintainer.getDataEmptyKeyValueCF(),
          indexMaintainer.getEmptyKeyValueQualifierForDataTable());
      }
      // Rebuild the index row from the corresponding the row in the the data table
      // Get the data row key from the index row key
      byte[] dataRowKey =
        indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(indexRowKey), viewConstants);
      buildIndexScanForDataTable.withStartRow(dataRowKey, true);
      buildIndexScanForDataTable.withStopRow(dataRowKey, true);
      buildIndexScanForDataTable.setTimeRange(0, maxTimestamp);
      // Pass the index row key to the partial index builder which will rebuild the index row and
      // check if the
      // row key of this rebuilt index row matches with the passed index row key
      buildIndexScanForDataTable.setAttribute(BaseScannerRegionObserverConstants.INDEX_ROW_KEY,
        indexRowKey);
      Result result = null;
      buildIndexScanForDataTable.setScanMetricsEnabled(isScanMetricsEnabled);
      try (ResultScanner resultScanner = dataHTable.getScanner(buildIndexScanForDataTable)) {
        result = resultScanner.next();
        if (isScanMetricsEnabled) {
          ScanMetrics scanMetrics = resultScanner.getScanMetrics();
          DataTableScanMetrics dataTableScanMetrics = buildDataTableScanMetrics(scanMetrics);
          dataTableScanMetrics.populateThreadLocalServerSideScanMetrics();
        }
      } catch (Throwable t) {
        ClientUtil.throwIOException(dataHTable.getName().toString(), t);
      }
      // A single cell will be returned. We decode that here
      byte[] value = result.value();
      long code = PLong.INSTANCE.getCodec().decodeLong(new ImmutableBytesWritable(value),
        SortOrder.getDefault());
      if (code == RebuildReturnCode.NO_DATA_ROW.getValue()) {
        // This means there does not exist a data table row for the data row key derived from
        // this unverified index row. So, no index row has been built
        // Delete the unverified row from index if it is old enough
        if (indexMaintainer.isAgedEnough(ts, ageThreshold)) {
          region.delete(indexMaintainer.createDelete(indexRowKey, ts, false));
        }
        // Skip this unverified row (i.e., do not return it to the client). Just retuning empty row
        // is
        // sufficient to do that
        row.clear();
        return;
      }
      // An index row has been built. Close the current scanner as the newly built row will not be
      // visible to it
      scanner.close();
      if (code == RebuildReturnCode.NO_INDEX_ROW.getValue()) {
        // This means there exists a data table row for the data row key derived from this
        // unverified index row
        // but the data table row does not point back to the index row.
        // Delete the unverified row from index if it is old enough
        if (indexMaintainer.isAgedEnough(ts, ageThreshold)) {
          region.delete(indexMaintainer.createDelete(indexRowKey, ts, false));
        }
        // Open a new scanner starting from the row after the current row
        reopenScanner(indexRowKey, false);
        hasMore = true;
        // Skip this unverified row (i.e., do not return it to the client). Just retuning empty row
        // is
        // sufficient to do that
        row.clear();
        return;
      }
      // code == RebuildReturnCode.INDEX_ROW_EXISTS.getValue()
      // Open a new scanner starting from the current row
      reopenScanner(indexRowKey, true);
      hasMore = scanner.next(row);
      streamReadSwitch.resultReturned(row);
      if (row.isEmpty()) {
        // This means the index row has been deleted before opening the new scanner.
        return;
      }
      if (isDummy(row)) {
        return;
      }
      // Check if the index row still exist after rebuild
      if (
        Bytes.compareTo(row.get(0).getRowArray(), row.get(0).getRowOffset(),
          row.get(0).getRowLength(), indexRowKey, 0, indexRowKey.length) != 0
      ) {
        // We got a different index row. This means either the unverified index row has
        // been deleted by the single row rebuild as part of the read repair process,
        // or the filters on the scan object skip the rebuilt index row.
        // If this new row is "verified" (or empty) then we are good to go.
        if (verifyRowAndRemoveEmptyColumn(row)) {
          return;
        }
        // The row is "unverified". Rewind the scanner and let the row be scanned again
        // so that it can be repaired
        scanner.close();
        reopenScanner(CellUtil.cloneRow(row.get(0)), true);
        hasMore = true;
        row.clear();
        return;
      }
      // The index row still exist after rebuild
      // Check if the index row is still unverified
      if (verifyRowAndRemoveEmptyColumn(row)) {
        // The index row status is "verified". This row is good to return to the client. We are done
        // here.
        return;
      }
      // The index row is still "unverified" after rebuild. This means that the data table row
      // timestamp is
      // lower than the timestamp of the unverified index row (ts) and the index row that is built
      // from
      // the data table row is masked by this unverified row. This happens if the first phase
      // updates (i.e.,
      // unverified index row updates) complete but the second phase updates (i.e., data table
      // updates) fail.
      // There could be back to back such events so we need a loop to go through them
      do {
        // First delete the unverified row from index if it is old enough
        if (indexMaintainer.isAgedEnough(ts, ageThreshold)) {
          region.delete(indexMaintainer.createDelete(indexRowKey, ts, true));
        }
        // Now we will do a single row scan to retrieve the verified index row built from the data
        // table row.
        // Note we cannot read all versions in one scan as the max number of row versions for an
        // index table
        // can be 1. In that case, we will get only one (i.e., the most recent) version instead of
        // all versions
        singleRowIndexScan.withStartRow(indexRowKey, true);
        singleRowIndexScan.withStopRow(indexRowKey, true);
        singleRowIndexScan.setTimeRange(minTimestamp, ts);
        setNewFilter(singleRowIndexScan, false);
        RegionScanner singleRowScanner =
          ((DelegateRegionScanner) delegate).getNewRegionScanner(singleRowIndexScan);
        row.clear();
        singleRowScanner.next(row);
        singleRowScanner.close();
        if (row.isEmpty()) {
          // The unverified row can match the filter while the repaired row does not. The shared
          // distinct prefix filter can also reject the repaired row, because an earlier row of
          // the scan can have the same prefix.
          return;
        }
        if (isDummy(row)) {
          return;
        }
        if (verifyRowAndRemoveEmptyColumn(row)) {
          // The index row status is "verified". This row is good to return to the client. We are
          // done here.
          return;
        }
        ts = getMaxTimestamp(row);
      } while (
        Bytes.compareTo(row.get(0).getRowArray(), row.get(0).getRowOffset(),
          row.get(0).getRowLength(), indexRowKey, 0, indexRowKey.length) == 0
      );
      // This should not happen at all
      Cell cell = row.get(0);
      byte[] rowKey = CellUtil.cloneRow(cell);
      throw new DoNotRetryIOException(
        "The scan returned a row with row key (" + Bytes.toStringBinary(rowKey)
          + ") different than indexRowKey (" + Bytes.toStringBinary(indexRowKey) + ") for table "
          + region.getRegionInfo().getTable().getNameAsString());
    }

    private boolean isEmptyColumn(Cell cell) {
      return Bytes.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        emptyCF, 0, emptyCF.length) == 0
        && Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(),
          cell.getQualifierLength(), emptyCQ, 0, emptyCQ.length) == 0;
    }

    private boolean verifyRowAndRemoveEmptyColumn(List<Cell> cellList) throws IOException {
      if (indexMaintainer.isUncovered()) {
        return true;
      }
      long cellListSize = cellList.size();
      Cell cell = null;
      if (cellListSize == 0) {
        return true;
      }
      Iterator<Cell> cellIterator = cellList.iterator();
      while (cellIterator.hasNext()) {
        cell = cellIterator.next();
        if (isEmptyColumn(cell)) {
          if (indexMaintainer instanceof TransformMaintainer) {
            // This is a transforming table. After cutoff, if there are new mutations on the table,
            // their empty col value would be x. So, we are only interested in unverified ones.
            if (
              Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                UNVERIFIED_BYTES, 0, UNVERIFIED_BYTES.length) == 0
            ) {
              return false;
            }
          } else {
            if (
              Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                VERIFIED_BYTES, 0, VERIFIED_BYTES.length) != 0
            ) {
              return false;
            }
          }
          return true;
        }
      }
      // This index row does not have an empty column cell. It must be removed by compaction. This
      // row will
      // be treated as unverified so that it can be repaired
      return false;
    }

    private long getMaxTimestamp(List<Cell> cellList) {
      long maxTs = 0;
      long ts = 0;
      Iterator<Cell> cellIterator = cellList.iterator();
      while (cellIterator.hasNext()) {
        Cell cell = cellIterator.next();
        ts = cell.getTimestamp();
        if (ts > maxTs) {
          maxTs = ts;
        }
      }
      return maxTs;
    }

    /**
     * @param cellList is an input and output parameter and will either include a valid row or be an
     *                 empty list
     * @return true if there exists more rows, otherwise false
     */
    private boolean verifyRowAndRepairIfNecessary(List<Cell> cellList) throws IOException {
      metricsSource.incrementIndexInspections(indexName);
      Cell cell = cellList.get(0);
      if (verifyRowAndRemoveEmptyColumn(cellList)) {
        return true;
      } else {
        long repairStart = EnvironmentEdgeManager.currentTimeMillis();

        byte[] rowKey = CellUtil.cloneRow(cell);
        long ts = getMaxTimestamp(cellList);
        cellList.clear();
        long repairTime;
        try {
          repairIndexRows(rowKey, ts, cellList);
          repairTime = EnvironmentEdgeManager.currentTimeMillis() - repairStart;
          metricsSource.incrementIndexRepairs(indexName);
          metricsSource.updateUnverifiedIndexRowAge(indexName,
            EnvironmentEdgeManager.currentTimeMillis() - ts);
          metricsSource.updateIndexRepairTime(indexName,
            EnvironmentEdgeManager.currentTimeMillis() - repairStart);
          if (shouldLog()) {
            LOG.info("Index row repair on region {} row ts {} took {} ms.",
              env.getRegionInfo().getRegionNameAsString(), ts, repairTime);
          }
        } catch (IOException e) {
          repairTime = EnvironmentEdgeManager.currentTimeMillis() - repairStart;
          metricsSource.incrementIndexRepairFailures(indexName);
          metricsSource.updateIndexRepairFailureTime(indexName,
            EnvironmentEdgeManager.currentTimeMillis() - repairStart);
          if (shouldLog()) {
            LOG.warn("Index row repair failure on region {} row ts {} took {} ms.",
              env.getRegionInfo().getRegionNameAsString(), ts, repairTime);
          }
          throw e;
        }

        if (cellList.isEmpty()) {
          // This means that the index row is invalid. Return false to tell the caller that this row
          // should be skipped
          return false;
        }
        return true;
      }
    }

    private boolean shouldLog() {
      if (loggingPercent == 0) {
        return false;
      }
      return (random.nextDouble() <= (loggingPercent / 100.0d));
    }
  }

  /**
   * Decides when an index scan switches from pread to stream read on a scanner that the checker
   * opens itself.
   * <p>
   * HBase starts a scan with pread. HBase switches a long scan to stream read only when it ships
   * the scanner, and it ships only the scanner that the RPC layer opens. The checker opens its own
   * scanners for read repair, and HBase never ships them. When such a scanner reads more than the
   * pread limit, HBase sets returnImmediately on the scanner context at each cell. Then each RPC
   * returns after one row.
   * <p>
   * This class finds that state and asks for one new scanner with stream read. The decision applies
   * to each scanner separately. A scanner that the checker opens for a later repair starts with
   * pread again, as each new scanner does in HBase. Thus many repairs close together stay on pread,
   * and each long scan after a repair costs at most one more reopen. The class also keeps the
   * position for the reopen, so that the new scanner does not return a row again or skip a row.
   */
  @VisibleForTesting
  static class StreamReadSwitch {
    private final boolean enabled;
    // The scan of the current scanner. It is null while the RPC layer owns the current scanner.
    private Scan currentScan;
    // The last row that the current scanner returned. A dummy row also marks a scan position.
    private byte[] lastRowKey;
    // The current scanner passed the pread limit and must change to stream read.
    private boolean pending;
    // The current scanner uses stream read.
    private boolean switched;

    StreamReadSwitch(Scan scan) {
      // HBase switches only a scan with the default read type. Pread and stream scans keep theirs.
      // The switch also needs the paging filter, because the restart position needs whole rows.
      enabled =
        scan.getReadType() == Scan.ReadType.DEFAULT && scan.getFilter() instanceof PagingFilter;
    }

    /**
     * Records a scanner that the checker opens with the given scan. The state of the previous
     * scanner does not apply to the new scanner.
     */
    void scannerOpened(Scan scan) {
      currentScan = scan;
      lastRowKey = null;
      pending = false;
      switched = scan.getReadType() == Scan.ReadType.STREAM;
    }

    /**
     * Records the position of the current scanner after a call to next.
     */
    void resultReturned(List<Cell> result) {
      if (currentScan != null && !result.isEmpty()) {
        lastRowKey = CellUtil.cloneRow(result.get(0));
      }
    }

    /**
     * Checks if the current scanner passed the pread limit during the last call to next. HBase
     * gives this signal only through returnImmediately on the scanner context. The flag stays set
     * until the RPC ends, so the check looks only for a change during the call. Server paging also
     * sets the flag when a page times out or the paging filter stops. These cases are not a pread
     * signal. The signal comes again in the next RPC, because the store scanner sets the flag at
     * each cell until it switches.
     */
    void checkPreadLimit(boolean wasReturnImmediately, ScannerContext scannerContext,
      long pageSizeMs, PagingFilter pagingFilter) {
      if (!enabled || currentScan == null || switched || pending || wasReturnImmediately) {
        return;
      }
      if (
        !PhoenixScannerContext.isReturnImmediately(scannerContext)
          || PhoenixScannerContext.isTimedOut(scannerContext, pageSizeMs)
          || (pagingFilter != null && pagingFilter.isStopped())
      ) {
        return;
      }
      pending = true;
    }

    boolean isPending() {
      return pending;
    }

    boolean isSwitched() {
      return switched;
    }

    /**
     * Creates the scan for the stream read scanner that replaces the current scanner. The new scan
     * is a copy, so the given index scan keeps its read type for later repairs. The new scan uses
     * the read point of the current scanner.
     */
    Scan createStreamScan(Scan indexScan, long mvccReadPoint) throws IOException {
      Scan streamScan = new Scan(indexScan);
      streamScan.setReadType(Scan.ReadType.STREAM);
      streamScan.withStartRow(getRestartRow(), isRestartRowInclusive());
      PackagePrivateFieldAccessor.setMvccReadPoint(streamScan, mvccReadPoint);
      return streamScan;
    }

    /**
     * Gets the start row for the stream read scanner. This is the last row that the current scanner
     * returned. If the current scanner did not return a row, this is the start row of its scan.
     */
    byte[] getRestartRow() {
      return lastRowKey != null ? lastRowKey : currentScan.getStartRow();
    }

    /**
     * Returns true if the stream read scanner includes its start row. The checker already used the
     * last returned row, so only the unread start row of the current scan is included. This
     * exclusive restart needs whole rows. The paging filter is a row filter, so HBase returns only
     * whole rows. Without a row filter, HBase can stop in a row at a size or time limit. Thus the
     * switch operates only on a scan with the paging filter.
     */
    boolean isRestartRowInclusive() {
      return lastRowKey == null && currentScan.includeStartRow();
    }
  }

  /**
   * Gets the index maintainer for the index table of the region from the scan attributes.
   */
  private static IndexMaintainer deserializeIndexMaintainer(Region region, Scan scan)
    throws IOException {
    byte[] indexTableNameBytes = region.getRegionInfo().getTable().getName();
    byte[] md = scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD);
    List<IndexMaintainer> maintainers = IndexMaintainer.deserialize(md, true);
    IndexMaintainer indexMaintainer = getIndexMaintainer(maintainers, indexTableNameBytes);
    if (indexMaintainer == null) {
      throw new DoNotRetryIOException("IndexMaintainer is not included in scan attributes for "
        + region.getRegionInfo().getTable().getNameAsString());
    }
    return indexMaintainer;
  }

  private static Filter getDelegateFilter(Scan scan) {
    Filter filter = scan.getFilter();
    return filter instanceof PagingFilter ? ((PagingFilter) filter).getDelegateFilter() : filter;
  }

  private static boolean needsUnverifiedRowFilter(Scan scan) {
    Filter delegateFilter = getDelegateFilter(scan);
    return delegateFilter != null && !(delegateFilter instanceof UnverifiedRowFilter);
  }

  /**
   * Wraps the scan filter of a covered index scan in an UnverifiedRowFilter. The scan filter can
   * reject an unverified row that read repair makes valid, so the wrapper passes unverified rows to
   * the checker. The PagingFilter stays the outermost filter. Returns true if the scan changes.
   */
  private static boolean addUnverifiedRowFilter(Scan scan, IndexMaintainer indexMaintainer) {
    if (!needsUnverifiedRowFilter(scan) || indexMaintainer.isUncovered()) {
      return false;
    }
    Filter filter = scan.getFilter();
    UnverifiedRowFilter unverifiedRowFilter = new UnverifiedRowFilter(getDelegateFilter(scan),
      scan.getAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_FAMILY_NAME),
      scan.getAttribute(BaseScannerRegionObserverConstants.EMPTY_COLUMN_QUALIFIER_NAME));
    if (filter instanceof PagingFilter) {
      ((PagingFilter) filter).setDelegateFilter(unverifiedRowFilter);
    } else {
      scan.setFilter(unverifiedRowFilter);
    }
    return true;
  }

  /**
   * Changes the index scan before HBase opens the region scanner. The checker removes the client
   * page filter, because rows that fail verification must not count against the row limit. In a
   * server merge, the uncovered index scanner gets the page filter instead. The checker also wraps
   * the scan filter for read repair. HBase then opens one region scanner for the scan and the
   * checker does not replace it. HBase ships only the scanner that it opens. Only a shipped scanner
   * can switch from pread to stream read on a long scan.
   */
  @Override
  public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan)
    throws IOException {
    boolean isCheckedScan = isRegionObserverFor(scan);
    if (isCheckedScan) {
      // In a server merge, the uncovered index scanner owns the row limit. The merge can drop a
      // verified index row, so the limit must apply after the merge. The superclass hook moves
      // the page filter to that scanner. Otherwise the checker owns the row limit.
      PageFilter pageFilter = UncoveredGlobalIndexRegionScanner.ownsRowLimit(scan)
        ? null
        : ScanUtil.removePageFilter(scan);
      scan.setAttribute(PAGE_SIZE_ATTRIB,
        pageFilter == null ? null : Bytes.toBytes(pageFilter.getPageSize()));
    }
    super.preScannerOpen(c, scan);
    if (!isCheckedScan) {
      return;
    }
    Region region = c.getEnvironment().getRegion();
    try {
      if (needsUnverifiedRowFilter(scan)) {
        addUnverifiedRowFilter(scan, deserializeIndexMaintainer(region, scan));
      }
    } catch (Throwable t) {
      // An exception other than IOException from a coprocessor hook can abort the region server.
      ClientUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
    }
  }

  @Override
  protected boolean isRegionObserverFor(Scan scan) {
    return scan.getAttribute(BaseScannerRegionObserverConstants.CHECK_VERIFY_COLUMN) != null;
  }

  /**
   * The checker runs before the scan observer. Thus the checker moves the row limit of a server
   * merge to the uncovered index scanner before it wraps the scan filter for read repair.
   */
  @Override
  protected boolean movesUncoveredIndexRowLimit(Scan scan) {
    return true;
  }

  @Override
  protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
    final Scan scan, final RegionScanner s) throws IOException, SQLException {
    // HBase calls preScannerOpen before this method, so the attribute holds a server value.
    byte[] pageSizeBytes = scan.getAttribute(PAGE_SIZE_ATTRIB);
    long pageSize = pageSizeBytes == null ? Long.MAX_VALUE : Bytes.toLong(pageSizeBytes);
    return new GlobalIndexScanner(c.getEnvironment(), scan, s, metricsSource, pageSize);
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    this.env = e;
    this.metricsSource = MetricsIndexerSourceFactory.getInstance().getGlobalIndexCheckerSource();
  }

}

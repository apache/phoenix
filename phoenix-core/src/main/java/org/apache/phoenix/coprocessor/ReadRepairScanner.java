package org.apache.phoenix.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.filter.PagedFilter;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EMPTY_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EMPTY_COLUMN_QUALIFIER_NAME;
import static org.apache.phoenix.util.ScanUtil.getDummyResult;
import static org.apache.phoenix.util.ScanUtil.getPageSizeMsForRegionScanner;
import static org.apache.phoenix.util.ScanUtil.isDummy;

public abstract class ReadRepairScanner extends BaseRegionScanner {

    public Logger LOGGER;
    public RegionScanner scanner;
    public Scan scan;
    public RegionCoprocessorEnvironment env;
    public byte[] emptyCF;
    public byte[] emptyCQ;
    public Region region;
    public boolean hasMore;
    public long pageSizeMs;
    public long pageSize = Long.MAX_VALUE;
    public long rowCount = 0;
    public long maxTimestamp;
    public long ageThreshold;
    public boolean restartScanDueToPageFilterRemoval = false;

    /*
    Scanner used for checking ground truth to help with read repair.
     */
    private Scan externalScanner = null;
    public Scan getExternalScanner() { return externalScanner; }

    public ReadRepairScanner(RegionCoprocessorEnvironment env, Scan scan, RegionScanner scanner) {
        super(scanner);
        LOGGER = LoggerFactory.getLogger(this.getClass());
        this.env = env;
        this.scan = scan;
        this.scanner = scanner;
        region = env.getRegion();
        emptyCF = scan.getAttribute(EMPTY_COLUMN_FAMILY_NAME);
        emptyCQ = scan.getAttribute(EMPTY_COLUMN_QUALIFIER_NAME);
        pageSizeMs = getPageSizeMsForRegionScanner(scan);
        maxTimestamp = scan.getTimeRange().getMax();
    }


    /*
    Method which checks whether a row is VERIFIED (i.e. does not need repair).
     */
    abstract boolean verifyRow(List<Cell> row);

    /*
    Method which repairs the given row
     */
    abstract void repairRow(List<Cell> row) throws IOException;

    public boolean next(List<Cell> result, boolean raw) throws IOException {
        try {
            long startTime = EnvironmentEdgeManager.currentTimeMillis();
            do {
                if (raw) {
                    hasMore = scanner.nextRaw(result);
                } else {
                    hasMore = scanner.next(result);
                }
                if (result.isEmpty()) {
                    return hasMore;
                }
                if (isDummy(result)) {
                    return true;
                }
                Cell cell = result.get(0);
                if (verifyRowAndRepairIfNecessary(result)) {
                    break;
                }
                if (hasMore && (EnvironmentEdgeManager.currentTimeMillis() - startTime) >= pageSizeMs) {
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
            ServerUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
            return false; // impossible
        }
    }

    @Override
    public boolean next(List<Cell> result) throws IOException {
        return next(result, false);
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        return next(result, true);
    }

    private boolean verifyRowAndRepairIfNecessary(List<Cell> cellList) throws IOException {
        // check if row is VERIFIED
        if (verifyRow(cellList)) {
            return true;
        }
        else {
            try {
                if (externalScanner == null) {
                    PageFilter pageFilter = removePageFilter(scan);
                    if (pageFilter != null) {
                        pageSize = pageFilter.getPageSize();
                        restartScanDueToPageFilterRemoval = true;
                    }
                    externalScanner = new Scan();
                }
                repairRow(cellList);
            } catch (IOException e) {
                LOGGER.warn("Row Repair failure on region {}.", env.getRegionInfo().getRegionNameAsString());
                throw e;
            }

            if (cellList.isEmpty()) {
                return false;
            }
            return true;
        }
    }

    private PageFilter removePageFilterFromFilterList(FilterList filterList) {
        Iterator<Filter> filterIterator = filterList.getFilters().iterator();
        while (filterIterator.hasNext()) {
            Filter filter = filterIterator.next();
            if (filter instanceof PageFilter) {
                filterIterator.remove();
                return (PageFilter) filter;
            } else if (filter instanceof FilterList) {
                PageFilter pageFilter = removePageFilterFromFilterList((FilterList) filter);
                if (pageFilter != null) {
                    return pageFilter;
                }
            }
        }
        return null;
    }

    // This method assumes that there is at most one instance of PageFilter in a scan
    private PageFilter removePageFilter(Scan scan) {
        Filter filter = scan.getFilter();
        if (filter != null) {
            if (filter instanceof PagedFilter) {
                filter = ((PagedFilter) filter).getDelegateFilter();
                if (filter == null) {
                    return null;
                }
            }
            if (filter instanceof PageFilter) {
                scan.setFilter(null);
                return (PageFilter) filter;
            } else if (filter instanceof FilterList) {
                return removePageFilterFromFilterList((FilterList) filter);
            }
        }
        return null;
    }

    public boolean isEmptyColumn(Cell cell) {
        return Bytes.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                emptyCF, 0, emptyCF.length) == 0 &&
                Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                        emptyCQ, 0, emptyCQ.length) == 0;
    }
}

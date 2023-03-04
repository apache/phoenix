package org.apache.phoenix.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.apache.phoenix.query.QueryConstants.VERIFIED_BYTES;
import static org.apache.phoenix.util.ScanUtil.getDummyResult;
import static org.apache.phoenix.util.ScanUtil.getPageSizeMsForRegionScanner;
import static org.apache.phoenix.util.ScanUtil.isDummy;
import static org.apache.phoenix.thirdparty.com.google.common.base.Preconditions.checkArgument;


/**
 * Coprocessor that verifies scanned rows of SYSTEM.CHILD_LINK table
 */
public class ChildLinkMetaDataObserver extends BaseScannerRegionObserver implements RegionCoprocessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChildLinkMetaDataObserver.class);

    /**
     * Class that verifies a given row of a SYSTEM.CHILD_LINK table.
     * An instance of this class is created for each scanner on the table
     * and used to verify individual rows.
     */
    public class ChildLinkMetaDataScanner extends BaseRegionScanner {

        private RegionScanner scanner;
        private Scan scan;
        private RegionCoprocessorEnvironment env;
        private Scan sysCatScan = null;
        private Scan childLinkScan;
        private byte[] emptyCF;
        private byte[] emptyCQ;
        private Region region;
        private boolean hasMore;
        private long pageSizeMs;
        private long pageSize = Long.MAX_VALUE;
        private long rowCount = 0;
        private long maxTimestamp;
        private long AGE_THRESHOLD = 1*60*60*1000; // 1 hour

        public ChildLinkMetaDataScanner(RegionCoprocessorEnvironment env,
                                        Scan scan,
                                        RegionScanner scanner) {
            super(scanner);
            this.env = env;
            this.scan = scan;
            this.scanner = scanner;
            region = env.getRegion();
            emptyCF = scan.getAttribute(EMPTY_COLUMN_FAMILY_NAME);
            emptyCQ = scan.getAttribute(EMPTY_COLUMN_QUALIFIER_NAME);
            pageSizeMs = getPageSizeMsForRegionScanner(scan);
            maxTimestamp = scan.getTimeRange().getMax();
        }

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
                        break;
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
            // check if empty column has VERIFIED status
            if (verifyRowAndRemoveEmptyColumn(cellList)) {
                return true;
            }
            else {
                Cell cell = cellList.get(0);
                byte[] rowKey = CellUtil.cloneRow(cell);
                long ts = cellList.get(0).getTimestamp();

                try {
                    repairChildLinkRow(rowKey, ts, cellList);
                } catch (IOException e) {
                    LOGGER.warn("Index row repair failure on region {}.", env.getRegionInfo().getRegionNameAsString());
                    throw e;
                }

                if (cellList.isEmpty()) {
                    return false;
                }
                return true;
            }
        }

        /*
        If the row is VERIFIED, remove the empty column from the row
         */
        private boolean verifyRowAndRemoveEmptyColumn(List<Cell> cellList) {
            long cellListSize = cellList.size();
            Cell cell = null;
            if (cellListSize == 0) {
                return true;
            }
            Iterator<Cell> cellIterator = cellList.iterator();
            while (cellIterator.hasNext()) {
                cell = cellIterator.next();
                if (isEmptyColumn(cell)) {
                    if (Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                            VERIFIED_BYTES, 0, VERIFIED_BYTES.length) != 0) {
                        return false;
                    }
                    // Empty column is not supposed to be returned to the client except
                    // when it is the only column included in the scan
                    if (cellListSize > 1) {
                        cellIterator.remove();
                    }
                    return true;
                }
            }
            // no empty column found
            return false;
        }

        /*
        Find parent link in syscat for given child link.
        If found, mark child link row VERIFIED and start a new scan from it.
        Otherwise, delete if row is old enough.
         */
        private void repairChildLinkRow(byte[] rowKey, long ts, List<Cell> row) throws IOException {
            Table sysCatHTable = ServerUtil.ConnectionFactory.
                    getConnection(ServerUtil.ConnectionType.DEFAULT_SERVER_CONNECTION, env).
                    getTable(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME));
            sysCatScan = new Scan();
            childLinkScan = new Scan(scan);

            // build syscat rowKey using given rowKey
            byte[] sysCatRowKey = getSysCatRowKey(rowKey);

            // scan syscat to find row
            sysCatScan.withStartRow(sysCatRowKey, true);
            sysCatScan.withStopRow(sysCatRowKey, true);
            sysCatScan.setTimeRange(0, maxTimestamp);
            Result result = null;
            try (ResultScanner resultScanner = sysCatHTable.getScanner(sysCatScan)){
                result = resultScanner.next();
            } catch (Throwable t) {
                ServerUtil.throwIOException(sysCatHTable.getName().toString(), t);
            }
            // if row found, repair and verifyRowAndRemoveEmptyColumn
            if (result != null && !result.isEmpty()) {
                markChildLinkVerified(rowKey, ts, region);
                scanner.close();
                childLinkScan.withStartRow(rowKey, true);
                scanner = ((BaseRegionScanner)delegate).getNewRegionScanner(childLinkScan);
                hasMore = true;
            }
            // if not, delete if old enough, otherwise ignore
            else {
                deleteIfAgedEnough(rowKey, ts, region);
            }
            row.clear();
        }

        /*
        Construct row key for SYSTEM.CATALOG from a given SYSTEM.CHILD_LINK row key
        SYSTEM.CATALOG -> (CHILD_TENANT_ID, CHILD_SCHEMA, CHILD_TABLE, PARENT_TENANT_ID, PARENT_FULL_NAME)
        SYSTEM.CHILD_LINK -> (PARENT_TENANT_ID, PARENT_SCHEMA, PARENT_TABLE, CHILD_TENANT_ID, CHILD_FULL_NAME)
         */
        private byte[] getSysCatRowKey(byte[] childLinkRowKey) {
            String NULL_DELIMITER = "\0";
            String[] childLinkRowKeyCols = new String(childLinkRowKey, StandardCharsets.UTF_8).split(NULL_DELIMITER);
            checkArgument(childLinkRowKeyCols.length == 5);
            String parentTenantId = childLinkRowKeyCols[0];
            String parentSchema = childLinkRowKeyCols[1];
            String parentTable = childLinkRowKeyCols[2];
            String childTenantId = childLinkRowKeyCols[3];
            String childFullName = childLinkRowKeyCols[4];

            String parentFullName = SchemaUtil.getTableName(parentSchema, parentTable);
            String childSchema = SchemaUtil.getSchemaNameFromFullName(childFullName);
            String childTable = SchemaUtil.getTableNameFromFullName(childFullName);

            String[] sysCatRowKeyCols = new String[] {childTenantId, childSchema, childTable, parentTenantId, parentFullName};
            return String.join(NULL_DELIMITER, sysCatRowKeyCols).getBytes(StandardCharsets.UTF_8);
        }

        private void deleteIfAgedEnough(byte[] rowKey, long ts, Region region) throws IOException {
            if ((EnvironmentEdgeManager.currentTimeMillis() - ts) > AGE_THRESHOLD) {
                Delete del = new Delete(rowKey);
                Mutation[] mutations = new Mutation[]{del};
                region.batchMutate(mutations);
            }
        }


        private void markChildLinkVerified(byte[] rowKey, long ts, Region region) throws IOException {
            Put put = new Put(rowKey);
            put.addColumn(emptyCF, emptyCQ, ts, VERIFIED_BYTES);
            Mutation[] mutations = new Mutation[]{put};
            region.batchMutate(mutations);
        }

        private boolean isEmptyColumn(Cell cell) {
            return Bytes.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                    emptyCF, 0, emptyCF.length) == 0 &&
                    Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                            emptyCQ, 0, emptyCQ.length) == 0;
        }
    }

    @Override
    protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan,
                                              final RegionScanner s) throws IOException, SQLException {
        return new ChildLinkMetaDataScanner(c.getEnvironment(), scan, s);
    }

    @Override
    protected boolean isRegionObserverFor(Scan scan) {
        return scan.getAttribute(CHECK_VERIFY_COLUMN) != null;
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

}

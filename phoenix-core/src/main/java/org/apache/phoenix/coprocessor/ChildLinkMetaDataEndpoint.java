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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.ChildLinkMetaDataProtos.CreateViewAddChildLinkRequest;
import org.apache.phoenix.coprocessor.generated.ChildLinkMetaDataProtos.ChildLinkMetaDataService;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;


import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.CHECK_VERIFY_COLUMN;
import static org.apache.phoenix.coprocessor.MetaDataEndpointImpl.mutateRowsWithLocks;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_BYTES;
import static org.apache.phoenix.query.QueryConstants.VERIFIED_BYTES;
import static org.apache.phoenix.thirdparty.com.google.common.base.Preconditions.checkArgument;


/**
 * Endpoint co-processor through which Phoenix metadata mutations for SYSTEM.CHILD_LINK flow.
 * The {@code parent->child } links ({@link org.apache.phoenix.schema.PTable.LinkType#CHILD_TABLE})
 * are stored in the SYSTEM.CHILD_LINK table.
 *
 * After PHOENIX-6141, this also serves as an observer coprocessor
 * that verifies scanned rows of SYSTEM.CHILD_LINK table.
 */
public class ChildLinkMetaDataEndpoint extends ChildLinkMetaDataService
        implements RegionCoprocessor, RegionObserver {

	private static final Logger LOGGER = LoggerFactory.getLogger(ChildLinkMetaDataEndpoint.class);
    private RegionCoprocessorEnvironment env;
    private PhoenixMetaDataCoprocessorHost phoenixAccessCoprocessorHost;
    private boolean accessCheckEnabled;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
        this.phoenixAccessCoprocessorHost = new PhoenixMetaDataCoprocessorHost(this.env);
        this.accessCheckEnabled = env.getConfiguration().getBoolean(QueryServices.PHOENIX_ACLS_ENABLED,
            QueryServicesOptions.DEFAULT_PHOENIX_ACLS_ENABLED);
    }

	@Override
	public Iterable<Service> getServices() {
		return Collections.singleton(this);
	}

    @Override
    public void createViewAddChildLink(RpcController controller,
            CreateViewAddChildLinkRequest request, RpcCallback<MetaDataResponse> done) {

        MetaDataResponse.Builder builder = MetaDataResponse.newBuilder();
        try {
            List<Mutation> childLinkMutations = ProtobufUtil.getMutations(request);
            if (childLinkMutations.isEmpty()) {
                done.run(builder.build());
                return;
            }
            byte[][] rowKeyMetaData = new byte[3][];
            MetaDataUtil.getTenantIdAndSchemaAndTableName(childLinkMutations, rowKeyMetaData);
            byte[] parentSchemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
            byte[] parentTableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
            String fullparentTableName = SchemaUtil.getTableName(parentSchemaName, parentTableName);

            getCoprocessorHost().preCreateViewAddChildLink(fullparentTableName);

            // From 4.15 the parent->child links are stored in a separate table SYSTEM.CHILD_LINK
            mutateRowsWithLocks(this.accessCheckEnabled, this.env.getRegion(), childLinkMutations,
                Collections.<byte[]>emptySet(), HConstants.NO_NONCE, HConstants.NO_NONCE);

        } catch (Throwable t) {
            LOGGER.error("Unable to write mutations to " +
                    PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME, t);
            builder.setReturnCode(MetaDataProtos.MutationCode.UNABLE_TO_CREATE_CHILD_LINK);
            builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
            done.run(builder.build());
        }
	}

	private PhoenixMetaDataCoprocessorHost getCoprocessorHost() {
		return phoenixAccessCoprocessorHost;
	}

    /**
     * Class that verifies a given row of a SYSTEM.CHILD_LINK table.
     * An instance of this class is created for each scanner on the table
     * and used to verify individual rows.
     */
    public class ChildLinkMetaDataScanner extends ReadRepairScanner {

        private Table sysCatHTable;
        private Scan childLinkScan;
        private Scan sysCatViewHeaderRowScan;
        private Scan sysCatChildParentLinkScan;
        private final String NULL_DELIMITER = "\0";

        public ChildLinkMetaDataScanner(RegionCoprocessorEnvironment env,
                                        Scan scan,
                                        RegionScanner scanner) throws IOException {
            super(env, scan, scanner);
            sysCatChildParentLinkScan = new Scan();
            setSysCatViewHeaderRowScan();
            ageThreshold = env.getConfiguration().getLong(
                    QueryServices.CHILD_LINK_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB,
                    QueryServicesOptions.DEFAULT_CHILD_LINK_ROW_AGE_THRESHOLD_TO_DELETE_MS);
            sysCatHTable = ServerUtil.ConnectionFactory.
                    getConnection(ServerUtil.ConnectionType.DEFAULT_SERVER_CONNECTION, env).
                    getTable(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME));
        }

        /*
        If the row is VERIFIED, remove the empty column from the row
         */
        @Override
        public boolean verifyRow(List<Cell> cellList) {
            long cellListSize = cellList.size();
            Cell cell = null;
            if (cellListSize == 0) {
                return true;
            }
            Iterator<Cell> cellIterator = cellList.iterator();
            while (cellIterator.hasNext()) {
                cell = cellIterator.next();
                if (isEmptyColumn(cell)) {
                    if (Bytes.compareTo(
                            cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                            VERIFIED_BYTES, 0, VERIFIED_BYTES.length) != 0
                        ) {
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
        @Override
        public void repairRow(List<Cell> row) throws IOException {
            Cell cell = row.get(0);
            byte[] rowKey = CellUtil.cloneRow(cell);
            long ts = row.get(0).getTimestamp();

            childLinkScan = new Scan(scan);

            boolean isChildParentLinkPresent = isRowPresentInSysCat(sysCatChildParentLinkScan,
                                                            getChildParentLinkSysCatRowKey(rowKey));

            boolean isViewHeaderRowPresent = false;
            if (isChildParentLinkPresent) {
                isViewHeaderRowPresent = isRowPresentInSysCat(sysCatViewHeaderRowScan,
                                                                getViewHeaderSysCatRowKey(rowKey));
            }
            // if row found, repair and verifyRowAndRemoveEmptyColumn
            if (isChildParentLinkPresent && isViewHeaderRowPresent) {
                markChildLinkVerified(rowKey, ts, region);
                scanner.close();
                childLinkScan.withStartRow(rowKey, true);
                scanner = region.getScanner(childLinkScan);
                hasMore = true;
            }
            // if not, delete if old enough, otherwise ignore
            else {
                deleteIfAgedEnough(rowKey, ts, region);
            }
            row.clear();
        }

        private boolean isRowPresentInSysCat(Scan scan, byte[] rowKey) throws IOException {
            scan.withStartRow(rowKey, true);
            scan.withStopRow(rowKey, true);
            scan.setTimeRange(0, maxTimestamp);

            Result result = null;
            try (ResultScanner resultScanner = sysCatHTable.getScanner(scan)) {
                result = resultScanner.next();
            }
            catch (Throwable t) {
                ServerUtil.throwIOException(sysCatHTable.getName().toString(), t);
            }

            return (result != null) && (!result.isEmpty());
        }

        /*
        Construct row key for SYSTEM.CATALOG view header row from a given SYSTEM.CHILD_LINK row key
         */
        private byte[] getViewHeaderSysCatRowKey(byte[] childLinkRowKey) {
            String[] childLinkRowKeyCols = new String(childLinkRowKey, StandardCharsets.UTF_8)
                                                .split(NULL_DELIMITER);
            checkArgument(childLinkRowKeyCols.length == 5);
            String childTenantId = childLinkRowKeyCols[3];
            String childFullName = childLinkRowKeyCols[4];

            String childSchema = SchemaUtil.getSchemaNameFromFullName(childFullName);
            String childTable = SchemaUtil.getTableNameFromFullName(childFullName);

            String[] sysCatRowKeyCols = new String[] {childTenantId, childSchema, childTable};
            return String.join(NULL_DELIMITER, sysCatRowKeyCols).getBytes(StandardCharsets.UTF_8);
        }

        /*
        Construct row key for SYSTEM.CATALOG parent->child link from a given SYSTEM.CHILD_LINK row key
        SYSTEM.CATALOG -> (CHILD_TENANT_ID, CHILD_SCHEMA, CHILD_TABLE, PARENT_TENANT_ID, PARENT_FULL_NAME)
        SYSTEM.CHILD_LINK -> (PARENT_TENANT_ID, PARENT_SCHEMA, PARENT_TABLE, CHILD_TENANT_ID, CHILD_FULL_NAME)
         */
        private byte[] getChildParentLinkSysCatRowKey(byte[] childLinkRowKey) {
            String[] childLinkRowKeyCols = new String(childLinkRowKey, StandardCharsets.UTF_8)
                                                .split(NULL_DELIMITER);
            checkArgument(childLinkRowKeyCols.length == 5);
            String parentTenantId = childLinkRowKeyCols[0];
            String parentSchema = childLinkRowKeyCols[1];
            String parentTable = childLinkRowKeyCols[2];
            String childTenantId = childLinkRowKeyCols[3];
            String childFullName = childLinkRowKeyCols[4];

            String parentFullName = SchemaUtil.getTableName(parentSchema, parentTable);
            String childSchema = SchemaUtil.getSchemaNameFromFullName(childFullName);
            String childTable = SchemaUtil.getTableNameFromFullName(childFullName);

            String[] sysCatRowKeyCols = new String[] {childTenantId, childSchema, childTable,
                                                        parentTenantId, parentFullName};
            return String.join(NULL_DELIMITER, sysCatRowKeyCols).getBytes(StandardCharsets.UTF_8);
        }


        private void deleteIfAgedEnough(byte[] rowKey, long ts, Region region) throws IOException {
            if ((EnvironmentEdgeManager.currentTimeMillis() - ts) > ageThreshold) {
                Delete del = new Delete(rowKey);
                Mutation[] mutations = new Mutation[]{del};
                region.batchMutate(mutations);
            }
        }


        private void markChildLinkVerified(byte[] rowKey, long ts, Region region) throws IOException {
            Put put = new Put(rowKey, ts);
            put.addColumn(emptyCF, emptyCQ, ts, VERIFIED_BYTES);
            Mutation[] mutations = new Mutation[]{put};
            region.batchMutate(mutations);
        }

        private void setSysCatViewHeaderRowScan() {
            sysCatViewHeaderRowScan = new Scan();
            sysCatViewHeaderRowScan.addColumn(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES);
            SingleColumnValueFilter tableTypeFilter = new SingleColumnValueFilter(
                    TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES, CompareOperator.EQUAL,
                    PTableType.VIEW.getSerializedValue().getBytes());
            sysCatViewHeaderRowScan.setFilter(tableTypeFilter);
        }

    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
                                         Scan scan, RegionScanner s) throws IOException {
        if (scan.getAttribute(CHECK_VERIFY_COLUMN) == null) {
            return s;
        }
        return new ChildLinkMetaDataScanner(c.getEnvironment(), scan, new PagingRegionScanner(c.getEnvironment().getRegion(), s, scan));
    }
}

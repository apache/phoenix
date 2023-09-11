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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.DynamicColumnMetaDataProtos;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.coprocessorclient.ScanRegionObserverConstants;
import org.apache.phoenix.hbase.index.metrics.GlobalIndexCheckerSource;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.iterate.NonAggregateRegionScannerFactory;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;

/**
 *
 * Wraps the scan performing a non aggregate query to prevent needless retries
 * if a Phoenix bug is encountered from our custom filter expression evaluation.
 * Unfortunately, until HBASE-7481 gets fixed, there's no way to do this from our
 * custom filters.
 *
 *
 * @since 0.1
 */
public class ScanRegionObserver extends BaseScannerRegionObserver implements RegionCoprocessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanRegionObserver.class);

    private static boolean readRepairTransformingTable = false;
    private static  GlobalIndexChecker.GlobalIndexScanner globalIndexScanner;
    private static GlobalIndexChecker globalIndexChecker = new GlobalIndexChecker();
    private static GlobalIndexCheckerSource metricsSource = MetricsIndexerSourceFactory.getInstance().getGlobalIndexCheckerSource();

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
            MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        try {
            preBatchMutateWithExceptions(miniBatchOp, c.getEnvironment().getRegion()
                    .getTableDescriptor().getTableName().getNameAsString());
        } catch(Throwable t) {
            // Wrap all exceptions in an IOException to prevent region server crashes
            throw ClientUtil.createIOException("Unable to Put cells corresponding to dynamic" +
                    "column metadata for " +
                    c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString(), t);
        }
    }

    /**
     * In case we are supporting exposing dynamic columns for wildcard queries, which is based on
     * the client-side config
     * {@link org.apache.phoenix.query.QueryServices#WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB},
     * we previously set attributes on the Put mutations where the key is the column family and
     * the value is the serialized list of dynamic columns.
     * Here we iterate over all Put mutations and add metadata for the list of dynamic columns for
     * each column family in its own cell under reserved qualifiers. See PHOENIX-374
     * @param miniBatchOp batch of mutations getting applied to region
     * @param tableName Name of table served by region
     * @throws IOException If an I/O error occurs when parsing protobuf
     */
    private void preBatchMutateWithExceptions(MiniBatchOperationInProgress<Mutation> miniBatchOp,
            String tableName)
    throws IOException {
        for (int i = 0; i < miniBatchOp.size(); i++) {
            Mutation m = miniBatchOp.getOperation(i);
            // There is at max 1 extra Put (for dynamic column shadow cells) per original Put
            Put dynColShadowCellsPut = null;
            if (m instanceof Put && Bytes.equals(m.getAttribute(
                    ScanRegionObserverConstants.DYNAMIC_COLUMN_METADATA_STORED_FOR_MUTATION), TRUE_BYTES)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Adding dynamic column metadata for table: " + tableName + ". Put :" +
                            m.toString());
                }
                NavigableMap<byte[], List<Cell>> famCellMap = m.getFamilyCellMap();
                for (byte[] fam : famCellMap.keySet()) {
                    byte[] serializedDynColsList = m.getAttribute(Bytes.toString(fam));
                    if (serializedDynColsList == null) {
                        // There are no dynamic columns for this column family
                        continue;
                    }
                    List<PTableProtos.PColumn> dynColsInThisFam = DynamicColumnMetaDataProtos.
                            DynamicColumnMetaData.parseFrom(serializedDynColsList)
                            .getDynamicColumnsList();
                    if (dynColsInThisFam.isEmpty()) {
                        continue;
                    }
                    if (dynColShadowCellsPut == null) {
                        dynColShadowCellsPut = new Put(m.getRow());
                    }
                    for (PTableProtos.PColumn dynColProto : dynColsInThisFam) {
                        // Add a column for this dynamic column to the metadata Put operation
                        dynColShadowCellsPut.addColumn(fam,
                                getQualifierForDynamicColumnMetaDataCell(dynColProto),
                                dynColProto.toByteArray());
                    }
                }
            }
            if (dynColShadowCellsPut != null) {
                miniBatchOp.addOperationsFromCP(i, new Mutation[]{dynColShadowCellsPut});
            }
        }
    }

    /**
     * We store the metadata for each dynamic cell in a separate cell in the same column family.
     * The column qualifier for this cell is:
     * {@link ScanRegionObserverConstants#DYN_COLS_METADATA_CELL_QUALIFIER} concatenated with the
     * qualifier of the actual dynamic column
     * @param dynColProto Protobuf representation of the dynamic column PColumn
     * @return Final qualifier for the metadata cell
     * @throws IOException If an I/O error occurs when parsing the byte array output stream
     */
    private static byte[] getQualifierForDynamicColumnMetaDataCell(PTableProtos.PColumn dynColProto)
    throws IOException {
        PColumn dynCol = PColumnImpl.createFromProto(dynColProto);
        ByteArrayOutputStream qual = new ByteArrayOutputStream();
        qual.write(ScanRegionObserverConstants.DYN_COLS_METADATA_CELL_QUALIFIER);
        qual.write(dynCol.getColumnQualifierBytes());
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Storing shadow cell for dynamic column metadata for dynamic column : " +
                    dynCol.getFamilyName().getString() + "." + dynCol.getName().getString());
        }
        return qual.toByteArray();
    }

    @Override
    protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan, final RegionScanner s) throws Throwable {
        NonAggregateRegionScannerFactory nonAggregateROUtil = new NonAggregateRegionScannerFactory(c.getEnvironment());
        if (scan.getAttribute(BaseScannerRegionObserverConstants.READ_REPAIR_TRANSFORMING_TABLE) != null) {
            readRepairTransformingTable = true;
            globalIndexScanner = globalIndexChecker.new GlobalIndexScanner(c.getEnvironment(), scan, s, metricsSource);
            return nonAggregateROUtil.getRegionScanner(scan, globalIndexScanner);
        }
        return nonAggregateROUtil.getRegionScanner(scan, s);
    }

    @Override
    protected boolean skipRegionBoundaryCheck(Scan scan) {
        return super.skipRegionBoundaryCheck(scan) || ScanUtil.isSimpleScan(scan);
    }

    @Override
    protected boolean isRegionObserverFor(Scan scan) {
        return ScanUtil.isNonAggregateScan(scan);
    }
}

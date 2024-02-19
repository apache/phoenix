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
package org.apache.phoenix.mapreduce.transform;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhoenixJobCounters;
import org.apache.phoenix.mapreduce.index.DirectHTableWriter;
import org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.transform.TransformMaintainer;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;

/**
 * Mapper that hands over rows from data table to the index table.
 */
public class PhoenixTransformRepairMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PhoenixTransformRepairMapper.class);
    private DirectHTableWriter writer;
    private PhoenixConnection connection;
    private ImmutableBytesPtr maintainers;
    private int batchSize;
    private List<Mutation> mutations ;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        final Configuration configuration = context.getConfiguration();
        writer = new DirectHTableWriter(configuration);
        try {
            final Properties overrideProps = new Properties();
            String scn = configuration.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
            String txScnValue = configuration.get(PhoenixConfigurationUtil.TX_SCN_VALUE);
            if(txScnValue==null && scn!=null) {
                overrideProps.put(PhoenixRuntime.BUILD_INDEX_AT_ATTRIB, scn);
            }
            connection = ConnectionUtil.getOutputConnection(configuration, overrideProps).unwrap(PhoenixConnection.class);
            maintainers=new ImmutableBytesPtr(PhoenixConfigurationUtil.getIndexMaintainers(configuration));
            int maxSize =
                    connection.getQueryServices().getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,
                            QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
            batchSize = Math.min(connection.getMutateBatchSize(), maxSize);
            this.mutations = Lists.newArrayListWithExpectedSize(batchSize);
            LOGGER.info("Mutation Batch Size = " + batchSize);
        } catch (SQLException e) {
            tryClosingResources();
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    protected void map(ImmutableBytesWritable row, Result value, Context context)
            throws IOException, InterruptedException {
        context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(1);
        String oldTableName = PhoenixConfigurationUtil.getIndexToolDataTableName(context.getConfiguration());
        Set<byte[]> extraRowsInNewTable = new HashSet<>();
        try (Table oldHTable = connection.getQueryServices().getTable(Bytes.toBytes(oldTableName)))  {
            for (Cell cell : value.rawCells()) {
                Scan buildNewTableScan = new Scan();
                // The following attributes are set to instruct UngroupedAggregateRegionObserver to do partial rebuild
                buildNewTableScan.setAttribute(BaseScannerRegionObserverConstants.UNGROUPED_AGG, TRUE_BYTES);
                buildNewTableScan.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, maintainers.get());
                buildNewTableScan.setAttribute(BaseScannerRegionObserverConstants.REBUILD_INDEXES, TRUE_BYTES);
                buildNewTableScan.setAttribute(BaseScannerRegionObserverConstants.SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));
                IndexMaintainer transformMaintainer = TransformMaintainer.deserialize(maintainers.get()).get(0);

                byte[] newRowKey = CellUtil.cloneRow(cell);
                // Rebuild the new row from the corresponding row in the old data table
                // To implement rowkey reordering etc, we need to rebuild the rowkey. For now it is the same
                buildNewTableScan.withStartRow(newRowKey, true);
                buildNewTableScan.withStopRow(newRowKey, true);
                buildNewTableScan.setTimeRange(0, cell.getTimestamp()+1);
                // Pass the index row key to the partial index builder which will rebuild the index row and check if the
                // row key of this rebuilt index row matches with the passed index row key
                buildNewTableScan.setAttribute(BaseScannerRegionObserverConstants.INDEX_ROW_KEY, newRowKey);
                Result result = null;
                try (ResultScanner resultScanner = oldHTable.getScanner(buildNewTableScan)) {
                    result = resultScanner.next();
                } catch (Throwable t) {
                    ClientUtil.throwIOException(oldTableName, t);
                }

                // A single cell will be returned. We decode that here
                byte[] scanVal = result.value();
                long code = PLong.INSTANCE.getCodec().decodeLong(new ImmutableBytesWritable(scanVal), SortOrder.getDefault());
                if (code == GlobalIndexChecker.RebuildReturnCode.NO_DATA_ROW.getValue()) {
                    if (!extraRowsInNewTable.contains(newRowKey)) {
                        extraRowsInNewTable.add(newRowKey);
                    }
                    // This means there does not exist an old table row for this unverified new table row
                    // Delete the unverified row from the new table
                    Delete del = transformMaintainer.buildRowDeleteMutation(newRowKey,
                            IndexMaintainer.DeleteType.ALL_VERSIONS, cell.getTimestamp());
                    mutations.add(del);
                }
                // Write Mutation Batch
                if (context.getCounter(PhoenixJobCounters.INPUT_RECORDS).getValue() % batchSize == 0) {
                    writeBatch(mutations, context);
                    mutations.clear();
                }
                context.getCounter(PhoenixIndexToolJobCounters.BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT).setValue(extraRowsInNewTable.size());
                // Make sure progress is reported to Application Master.
                context.progress();
            }
        } catch (SQLException e) {
            LOGGER.error(" Error {}  while read/write of a record ", e.getMessage());
            context.getCounter(PhoenixJobCounters.FAILED_RECORDS).increment(1);
            throw new RuntimeException(e);
        }
    }

    private void writeBatch(List<Mutation> mutations, Context context)
            throws IOException, SQLException, InterruptedException {
        writer.write(mutations);
        context.getCounter(PhoenixJobCounters.OUTPUT_RECORDS).increment(mutations.size());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
            // Write the last & final Mutation Batch
            if (!mutations.isEmpty()) {
                writeBatch(mutations, context);
            }
            // We are writing some dummy key-value as map output here so that we commit only one
            // output to reducer.
            context.write(new ImmutableBytesWritable(UUID.randomUUID().toString().getBytes()),
                new IntWritable(0));
            super.cleanup(context);
        } catch (SQLException e) {
            LOGGER.error(" Error {}  while read/write of a record ", e.getMessage());
            context.getCounter(PhoenixJobCounters.FAILED_RECORDS).increment(1);
            throw new RuntimeException(e);
        } finally {
            tryClosingResources();
        }
    }

    private void tryClosingResources() throws IOException {
        if (this.connection != null) {
            try {
                this.connection.close();
            } catch (SQLException e) {
                LOGGER.error("Error while closing connection in the PhoenixIndexMapper class ", e);
            }
        }
        if (this.writer != null) {
            this.writer.close();
        }
    }

}

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
package org.apache.phoenix.mapreduce.index;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.phoenix.cache.ServerCacheClient;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhoenixJobCounters;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * Mapper that hands over rows from data table to the index table.
 */
public class PhoenixIndexPartialBuildMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PhoenixIndexPartialBuildMapper.class);

    private PhoenixConnection connection;

    private DirectHTableWriter writer;

    private int batchSize;

    private List<Mutation> mutations ;
    
    private ImmutableBytesPtr maintainers;

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
            connection.setAutoCommit(false);
            // Get BatchSize
            ConnectionQueryServices services = connection.getQueryServices();
            int maxSize =
                    services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,
                        QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
            batchSize = Math.min(connection.getMutateBatchSize(), maxSize);
            LOGGER.info("Mutation Batch Size = " + batchSize);
            this.mutations = Lists.newArrayListWithExpectedSize(batchSize);
            maintainers=new ImmutableBytesPtr(PhoenixConfigurationUtil.getIndexMaintainers(configuration));
        } catch (SQLException e) {
            tryClosingResources();
            throw new RuntimeException(e.getMessage());
        } 
    }

    @Override
    protected void map(ImmutableBytesWritable row, Result value, Context context)
            throws IOException, InterruptedException {
        context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(1);
        try {
            byte[] attribValue = ByteUtil.copyKeyBytesIfNecessary(maintainers);
            byte[] uuidValue = ServerCacheClient.generateId();
            byte[] clientVersion = Bytes.toBytes(MetaDataProtocol.PHOENIX_VERSION);
            Put put = null;
            Delete del = null;
            for (Cell cell : value.rawCells()) {
                if (cell.getType() == Cell.Type.Put) {
                    if (put == null) {
                        put = new Put(CellUtil.cloneRow(cell));
                        put.setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
                        put.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, attribValue);
                        put.setAttribute(BaseScannerRegionObserverConstants.CLIENT_VERSION, clientVersion);
                        put.setAttribute(BaseScannerRegionObserverConstants.REPLAY_WRITES, BaseScannerRegionObserverConstants.REPLAY_ONLY_INDEX_WRITES);
                        mutations.add(put);
                    }
                    put.add(cell);
                } else {
                    if (del == null) {
                        del = new Delete(CellUtil.cloneRow(cell));
                        del.setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
                        del.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, attribValue);
                        del.setAttribute(BaseScannerRegionObserverConstants.CLIENT_VERSION, clientVersion);
                        del.setAttribute(BaseScannerRegionObserverConstants.REPLAY_WRITES, BaseScannerRegionObserverConstants.REPLAY_ONLY_INDEX_WRITES);
                        mutations.add(del);
                    }
                    del.add(cell);
                }
            }
            // Write Mutation Batch
            if (context.getCounter(PhoenixJobCounters.INPUT_RECORDS).getValue() % batchSize == 0) {
                writeBatch(mutations, context);
                mutations.clear();
            }
            // Make sure progress is reported to Application Master.
            context.progress();
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
            context.write(new ImmutableBytesWritable(
                    UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)),
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

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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhoenixJobCounters;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapper that hands over rows from data table to the index table.
 */
public class PhoenixIndexImportDirectMapper extends
        Mapper<NullWritable, PhoenixIndexDBWritable, ImmutableBytesWritable, IntWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixIndexImportDirectMapper.class);

    private final PhoenixIndexDBWritable indxWritable = new PhoenixIndexDBWritable();

    private List<ColumnInfo> indxTblColumnMetadata;

    private Connection connection;

    private PreparedStatement pStatement;

    private DirectHTableWriter writer;

    private int batchSize;
    private long batchSizeBytes;

    private MutationState mutationState;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        final Configuration configuration = context.getConfiguration();
        writer = new DirectHTableWriter(configuration);

        try {
            indxTblColumnMetadata =
                    PhoenixConfigurationUtil.getUpsertColumnMetadataList(configuration);
            indxWritable.setColumnMetadata(indxTblColumnMetadata);

            final Properties overrideProps = new Properties();
            String scn = configuration.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
            String txScnValue = configuration.get(PhoenixConfigurationUtil.TX_SCN_VALUE);
            if(txScnValue==null) {
                overrideProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, scn);
            }
            connection = ConnectionUtil.getOutputConnection(configuration, overrideProps);
            connection.setAutoCommit(false);
            // Get BatchSize, which is in terms of rows
            ConnectionQueryServices services = ((PhoenixConnection) connection).getQueryServices();
            int maxSize =
                    services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,
                        QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
            batchSize = Math.min(((PhoenixConnection) connection).getMutateBatchSize(), maxSize);

            //Get batch size in terms of bytes
            batchSizeBytes = ((PhoenixConnection) connection).getMutateBatchSizeBytes();

            LOG.info("Mutation Batch Size = " + batchSize);

            final String upsertQuery = PhoenixConfigurationUtil.getUpsertStatement(configuration);
            this.pStatement = connection.prepareStatement(upsertQuery);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void map(NullWritable key, PhoenixIndexDBWritable record, Context context)
            throws IOException, InterruptedException {

        try {
            final List<Object> values = record.getValues();
            indxWritable.setValues(values);
            indxWritable.write(this.pStatement);
            this.pStatement.execute();

            final PhoenixConnection pconn = connection.unwrap(PhoenixConnection.class);
            MutationState currentMutationState = pconn.getMutationState();
            if (mutationState == null) {
                mutationState = currentMutationState;
            }
            // Keep accumulating Mutations till batch size
            mutationState.join(currentMutationState);

            // Write Mutation Batch
            if (context.getCounter(PhoenixJobCounters.INPUT_RECORDS).getValue() % batchSize == 0) {
                writeBatch(mutationState, context);
                mutationState = null;
            }

            // Make sure progress is reported to Application Master.
            context.progress();
        } catch (SQLException e) {
            LOG.error(" Error {}  while read/write of a record ", e.getMessage());
            context.getCounter(PhoenixJobCounters.FAILED_RECORDS).increment(1);
            throw new RuntimeException(e);
        }
        context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(1);
    }

    private void writeBatch(MutationState mutationState, Context context) throws IOException,
            SQLException, InterruptedException {
        final Iterator<Pair<byte[], List<Mutation>>> iterator = mutationState.toMutations(true, null);
        while (iterator.hasNext()) {
            Pair<byte[], List<Mutation>> mutationPair = iterator.next();
            List<Mutation> batchMutations = mutationPair.getSecond();
            List<List<Mutation>> batchOfBatchMutations =
                MutationState.getMutationBatchList(batchSize, batchSizeBytes, batchMutations);
            for (List<Mutation> mutationList : batchOfBatchMutations) {
                writer.write(mutationList);
            }
            context.getCounter(PhoenixJobCounters.OUTPUT_RECORDS).increment(
                mutationPair.getSecond().size());
        }
        connection.rollback();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
            // Write the last & final Mutation Batch
            if (mutationState != null) {
                writeBatch(mutationState, context);
            }
            // We are writing some dummy key-value as map output here so that we commit only one
            // output to reducer.
            context.write(new ImmutableBytesWritable(UUID.randomUUID().toString().getBytes()),
                new IntWritable(0));
            super.cleanup(context);
        } catch (SQLException e) {
            LOG.error(" Error {}  while read/write of a record ", e.getMessage());
            context.getCounter(PhoenixJobCounters.FAILED_RECORDS).increment(1);
            throw new RuntimeException(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOG.error("Error {} while closing connection in the PhoenixIndexMapper class ",
                        e.getMessage());
                }
            }
            if (writer != null) {
                writer.close();
            }
        }
    }
}

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
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhoenixJobCounters;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
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

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        final Configuration configuration = context.getConfiguration();
        writer = new DirectHTableWriter(configuration);

        try {
            indxTblColumnMetadata =
                    PhoenixConfigurationUtil
                            .getUpsertColumnMetadataList(configuration);
            indxWritable.setColumnMetadata(indxTblColumnMetadata);

            final Properties overrideProps = new Properties();
            overrideProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                configuration.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE));
            connection = ConnectionUtil.getOutputConnection(configuration, overrideProps);
            connection.setAutoCommit(false);
            final String upsertQuery = PhoenixConfigurationUtil.getUpsertStatement(configuration);
            this.pStatement = connection.prepareStatement(upsertQuery);

        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    protected void map(NullWritable key, PhoenixIndexDBWritable record, Context context)
            throws IOException, InterruptedException {

        context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(1);

        try {
            final List<Object> values = record.getValues();
            indxWritable.setValues(values);
            indxWritable.write(this.pStatement);
            this.pStatement.execute();

            final PhoenixConnection pconn = connection.unwrap(PhoenixConnection.class);
            final Iterator<Pair<byte[], List<Mutation>>> iterator =
                    pconn.getMutationState().toMutations(true);

            while (iterator.hasNext()) {
                Pair<byte[], List<Mutation>> mutationPair = iterator.next();
                for (Mutation mutation : mutationPair.getSecond()) {
                    writer.write(mutation);
                }
                context.getCounter(PhoenixJobCounters.OUTPUT_RECORDS).increment(1);
            }
            connection.rollback();
        } catch (SQLException e) {
            LOG.error(" Error {}  while read/write of a record ", e.getMessage());
            context.getCounter(PhoenixJobCounters.FAILED_RECORDS).increment(1);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // We are writing some dummy key-value as map output here so that we commit only one
        // output to reducer.
        context.write(new ImmutableBytesWritable(UUID.randomUUID().toString().getBytes()),
            new IntWritable(0));
        super.cleanup(context);
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.error("Error {} while closing connection in the PhoenixIndexMapper class ",
                    e.getMessage());
            }
        }
        writer.close();
    }
}

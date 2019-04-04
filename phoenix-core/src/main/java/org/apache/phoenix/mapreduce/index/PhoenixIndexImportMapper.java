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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.ImportPreUpsertKeyValueProcessor;
import org.apache.phoenix.mapreduce.PhoenixJobCounters;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.transaction.PhoenixTransactionProvider;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Mapper that hands over rows from data table to the index table.
 *
 */
public class PhoenixIndexImportMapper extends Mapper<NullWritable, PhoenixIndexDBWritable, ImmutableBytesWritable, KeyValue> {

    private static final Logger logger = LoggerFactory.getLogger(PhoenixIndexImportMapper.class);
    
    private final PhoenixIndexDBWritable indxWritable = new PhoenixIndexDBWritable();
    
    private List<ColumnInfo> indxTblColumnMetadata ;
    
    private Connection connection;
    
    private String indexTableName;
    
    private ImportPreUpsertKeyValueProcessor preUpdateProcessor;
    
    private PreparedStatement pStatement;
    
    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        final Configuration configuration = context.getConfiguration();
        try {
            indxTblColumnMetadata = PhoenixConfigurationUtil.getUpsertColumnMetadataList(context.getConfiguration());
            indxWritable.setColumnMetadata(indxTblColumnMetadata);
            
            preUpdateProcessor = PhoenixConfigurationUtil.loadPreUpsertProcessor(configuration);
            indexTableName = PhoenixConfigurationUtil.getPhysicalTableName(configuration);
            final Properties overrideProps = new Properties ();
            String scn = configuration.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
            String txScnValue = configuration.get(PhoenixConfigurationUtil.TX_SCN_VALUE);
            if(txScnValue==null) {
                overrideProps.put(PhoenixRuntime.BUILD_INDEX_AT_ATTRIB, scn);
            }
            connection = ConnectionUtil.getOutputConnection(configuration,overrideProps);
            connection.setAutoCommit(false);
            final String upsertQuery = PhoenixConfigurationUtil.getUpsertStatement(configuration);
            this.pStatement = connection.prepareStatement(upsertQuery);
            
        } catch (SQLException e) {
            tryClosingConnection();
            throw new RuntimeException(e.getMessage());
        } 
    }
    
    @Override
    protected void map(NullWritable key, PhoenixIndexDBWritable record, Context context)
            throws IOException, InterruptedException {
       
        context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(1);
        
        PhoenixTransactionProvider provider = null;
        Configuration conf = context.getConfiguration();
        long ts = HConstants.LATEST_TIMESTAMP;
        String txnIdStr = conf.get(PhoenixConfigurationUtil.TX_SCN_VALUE);
        if (txnIdStr != null) {
            ts = Long.parseLong(txnIdStr);
            provider = TransactionFactory.Provider.getDefault().getTransactionProvider();
            String txnProviderStr = conf.get(PhoenixConfigurationUtil.TX_PROVIDER);
            if (txnProviderStr != null) {
                provider = TransactionFactory.Provider.valueOf(txnProviderStr).getTransactionProvider();
            }
        }
        try {
           final ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
           final List<Object> values = record.getValues();
           indxWritable.setValues(values);
           indxWritable.write(this.pStatement);
           this.pStatement.execute();
            
           PhoenixConnection pconn = connection.unwrap(PhoenixConnection.class);
           final Iterator<Pair<byte[],List<Mutation>>> iterator = pconn.getMutationState().toMutations(true);
           while (iterator.hasNext()) {
                Pair<byte[], List<Mutation>> pair = iterator.next();
                if (Bytes.compareTo(Bytes.toBytes(indexTableName), pair.getFirst()) != 0) {
                    // skip edits for other tables
                    continue;
                }
                List<Cell> keyValues = Lists.newArrayListWithExpectedSize(pair.getSecond().size() * 5); // Guess-timate 5 key values per row
                for (Mutation mutation : pair.getSecond()) {
                    if (mutation instanceof Put) {
                        if (provider != null) {
                            mutation = provider.markPutAsCommitted((Put)mutation, ts, ts);
                        }
                        for (List<Cell> cellList : mutation.getFamilyCellMap().values()) {
                            List<Cell>keyValueList = preUpdateProcessor.preUpsert(mutation.getRow(), cellList);
                            for (Cell keyValue : keyValueList) {
                                keyValues.add(keyValue);
                            }
                        }
                    }
                }
                Collections.sort(keyValues, pconn.getKeyValueBuilder().getKeyValueComparator());
                for (Cell kv : keyValues) {
                    outputKey.set(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength());
                    context.write(outputKey, PhoenixKeyValueUtil.maybeCopyCell(kv));
                }
                context.getCounter(PhoenixJobCounters.OUTPUT_RECORDS).increment(1);
            }
            connection.rollback();
       } catch (SQLException e) {
           logger.error("Error {}  while read/write of a record ",e.getMessage());
           context.getCounter(PhoenixJobCounters.FAILED_RECORDS).increment(1);
           throw new RuntimeException(e);
        } 
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        tryClosingConnection();
    }

    private void tryClosingConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error("Error while closing connection in the PhoenixIndexMapper class ", e);
            }
        }
    }
}

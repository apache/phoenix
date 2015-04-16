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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.mapreduce.ImportPreUpsertKeyValueProcessor;
import org.apache.phoenix.mapreduce.PhoenixJobCounters;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapper that hands over rows from data table to the index table.
 *
 */
public class PhoenixIndexImportMapper extends Mapper<NullWritable, PhoenixIndexDBWritable, ImmutableBytesWritable, KeyValue> {

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixIndexImportMapper.class);
    
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
            indexTableName = PhoenixConfigurationUtil.getOutputTableName(configuration);
            final Properties overrideProps = new Properties ();
            overrideProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, configuration.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE));
            connection = ConnectionUtil.getOutputConnection(configuration,overrideProps);
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
           final ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
           final List<Object> values = record.getValues();
           indxWritable.setValues(values);
           indxWritable.write(this.pStatement);
           this.pStatement.execute();
            
           final Iterator<Pair<byte[], List<KeyValue>>> uncommittedDataIterator = PhoenixRuntime.getUncommittedDataIterator(connection, true);
           while (uncommittedDataIterator.hasNext()) {
                Pair<byte[], List<KeyValue>> kvPair = uncommittedDataIterator.next();
                if (Bytes.compareTo(Bytes.toBytes(indexTableName), kvPair.getFirst()) != 0) {
                    // skip edits for other tables
                    continue;
                }
                List<KeyValue> keyValueList = kvPair.getSecond();
                keyValueList = preUpdateProcessor.preUpsert(kvPair.getFirst(), keyValueList);
                for (KeyValue kv : keyValueList) {
                    outputKey.set(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength());
                    context.write(outputKey, kv);
                }
                context.getCounter(PhoenixJobCounters.OUTPUT_RECORDS).increment(1);
            }
            connection.rollback();
       } catch (SQLException e) {
           LOG.error(" Error {}  while read/write of a record ",e.getMessage());
           context.getCounter(PhoenixJobCounters.FAILED_RECORDS).increment(1);
           throw new RuntimeException(e);
        } 
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
         super.cleanup(context);
         if(connection != null) {
             try {
                connection.close();
            } catch (SQLException e) {
                LOG.error("Error {} while closing connection in the PhoenixIndexMapper class ",e.getMessage());
            }
         }
    }
}
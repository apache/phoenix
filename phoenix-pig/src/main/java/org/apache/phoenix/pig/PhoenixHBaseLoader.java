/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.pig;

import static org.apache.commons.lang.StringUtils.isEmpty;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.phoenix.pig.hadoop.PhoenixInputFormat;
import org.apache.phoenix.pig.hadoop.PhoenixRecord;
import org.apache.phoenix.pig.util.PhoenixPigSchemaUtil;
import org.apache.phoenix.pig.util.QuerySchemaParserFunction;
import org.apache.phoenix.pig.util.TableSchemaParserFunction;
import org.apache.phoenix.pig.util.TypeUtil;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import com.google.common.base.Preconditions;

/**
 * LoadFunc to load data from HBase using Phoenix .
 * 
 * Example usage: 
 * a) TABLE
 *   i)   A = load 'hbase://table/HIRES'  using
 * org.apache.phoenix.pig.PhoenixHBaseLoader('localhost');
 *               
 *       The above loads the data from a table 'HIRES'
 *       
 *   ii)  A = load 'hbase://table/HIRES/id,name' using
 *       org.apache.phoenix.pig.PhoenixHBaseLoader('localhost');
 *       
 *       Here, only id, name are returned from the table HIRES as part of LOAD.
 * 
 * b)  QUERY
 *   i)   B = load 'hbase://query/SELECT fname, lname FROM HIRES' using
 *             org.apache.phoenix.pig.PhoenixHBaseLoader('localhost');
 *       
 *        The above loads fname and lname columns from 'HIRES' table.
 * 
 */
public final class PhoenixHBaseLoader extends LoadFunc implements LoadMetadata {

    private static final Log LOG = LogFactory.getLog(PhoenixHBaseLoader.class);
    private static final String PHOENIX_TABLE_NAME_SCHEME = "hbase://table/";
    private static final String PHOENIX_QUERY_SCHEME      = "hbase://query/";
    private static final String RESOURCE_SCHEMA_SIGNATURE = "phoenix.pig.schema";
   
    private PhoenixPigConfiguration config;
    private String tableName;
    private String selectQuery;
    private String zkQuorum ;
    private PhoenixInputFormat inputFormat;
    private RecordReader<NullWritable, PhoenixRecord> reader;
    private String contextSignature;
    private ResourceSchema schema;
       
    /**
     * @param zkQuorum
     */
    public PhoenixHBaseLoader(String zkQuorum) {
        super();
        Preconditions.checkNotNull(zkQuorum);
        Preconditions.checkState(zkQuorum.length() > 0, "Zookeeper quorum cannot be empty!");
        this.zkQuorum = zkQuorum;
    }
    
    @Override
    public void setLocation(String location, Job job) throws IOException {        
        final Configuration configuration = job.getConfiguration();
        //explicitly turning off combining splits. 
        configuration.setBoolean("pig.noSplitCombination", true);
        this.initializePhoenixPigConfiguration(location, configuration);
    }

    /**
     * Initialize PhoenixPigConfiguration if it is null. Called by {@link #setLocation} and {@link #getSchema}
     * @param location
     * @param configuration
     * @throws PigException
     */
    private void initializePhoenixPigConfiguration(final String location, final Configuration configuration) throws PigException {
        if(this.config != null) {
            return;
        }
        this.config = new PhoenixPigConfiguration(configuration);
        this.config.setServerName(this.zkQuorum);
        Pair<String,String> pair = null;
        try {
            if (location.startsWith(PHOENIX_TABLE_NAME_SCHEME)) {
                String tableSchema = location.substring(PHOENIX_TABLE_NAME_SCHEME.length());
                final TableSchemaParserFunction parseFunction = new TableSchemaParserFunction();
                pair =  parseFunction.apply(tableSchema);
             } else if (location.startsWith(PHOENIX_QUERY_SCHEME)) {
                this.selectQuery = location.substring(PHOENIX_QUERY_SCHEME.length());
                final QuerySchemaParserFunction queryParseFunction = new QuerySchemaParserFunction(this.config);
                pair = queryParseFunction.apply(this.selectQuery);
                config.setSelectStatement(this.selectQuery);
            }
            this.tableName = pair.getFirst();
            final String selectedColumns = pair.getSecond();
            
            if(isEmpty(this.tableName) && isEmpty(this.selectQuery)) {
                printUsage(location);
            }
            this.config.setTableName(this.tableName);
            if(!isEmpty(selectedColumns)) {
                this.config.setSelectColumns(selectedColumns);    
            }
        } catch(IllegalArgumentException iae) {
            printUsage(location);
        } 
    }

  
    @Override
    public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
        return location;
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        if(inputFormat == null) {
            inputFormat = new PhoenixInputFormat();
        }
        return inputFormat;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = reader;
        final String resourceSchemaAsStr = getValueFromUDFContext(this.contextSignature,RESOURCE_SCHEMA_SIGNATURE);
        if (resourceSchemaAsStr == null) {
            throw new IOException("Could not find schema in UDF context");
        }
       schema = (ResourceSchema)ObjectSerializer.deserialize(resourceSchemaAsStr); 
    }

     /*
     * @see org.apache.pig.LoadFunc#setUDFContextSignature(java.lang.String)
     */
    @Override
    public void setUDFContextSignature(String signature) {
        this.contextSignature = signature;
    }
    
    @Override
    public Tuple getNext() throws IOException {
        try {
            if(!reader.nextKeyValue()) {
               return null; 
            }
            final PhoenixRecord phoenixRecord = reader.getCurrentValue();
            if(phoenixRecord == null) {
                return null;
            }
            final Tuple tuple = TypeUtil.transformToTuple(phoenixRecord,schema.getFields());
            return tuple;
       } catch (InterruptedException e) {
            int errCode = 6018;
            final String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,PigException.REMOTE_ENVIRONMENT, e);
        }
    }
    
    private void printUsage(final String location) throws PigException {
        String locationErrMsg = String.format("The input location in load statement should be of the form " +
                "%s<table name> or %s<query>. Got [%s] ",PHOENIX_TABLE_NAME_SCHEME,PHOENIX_QUERY_SCHEME,location);
        LOG.error(locationErrMsg);
        throw new PigException(locationErrMsg);
    }
    
    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
        if(schema != null) {
            return schema;
        }
        final Configuration configuration = job.getConfiguration();
        this.initializePhoenixPigConfiguration(location, configuration);
        this.schema = PhoenixPigSchemaUtil.getResourceSchema(this.config);
        if(LOG.isDebugEnabled()) {
            LOG.debug(String.format("Resource Schema generated for location [%s] is [%s]", location, schema.toString()));
        }
        this.storeInUDFContext(this.contextSignature, RESOURCE_SCHEMA_SIGNATURE, ObjectSerializer.serialize(schema));
        return schema;
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException {
       // not implemented
        return null;
    }

    @Override
    public String[] getPartitionKeys(String location, Job job) throws IOException {
     // not implemented
        return null;
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter) throws IOException {
     // not implemented
    }
 
    private void storeInUDFContext(final String signature,final String key,final String value) {
        final UDFContext udfContext = UDFContext.getUDFContext();
        final Properties props = udfContext.getUDFProperties(this.getClass(), new String[]{signature});
        props.put(key, value);
    }
    
    private String getValueFromUDFContext(final String signature,final String key) {
        final UDFContext udfContext = UDFContext.getUDFContext();
        final Properties props = udfContext.getUDFProperties(this.getClass(), new String[]{signature});
        return props.getProperty(key);
    }
}

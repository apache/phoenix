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
package org.apache.phoenix.pig.hadoop;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.pig.PhoenixPigConfiguration;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ScanUtil;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * The InputFormat class for generating the splits and creating the record readers.
 * 
 */
public final class PhoenixInputFormat extends InputFormat<NullWritable, PhoenixRecord> {

    private static final Log LOG = LogFactory.getLog(PhoenixInputFormat.class);
    private PhoenixPigConfiguration phoenixConfiguration;
    private Connection connection;
    private QueryPlan  queryPlan;
    
    /**
     * instantiated by framework
     */
    public PhoenixInputFormat() {
    }

    @Override
    public RecordReader<NullWritable, PhoenixRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {       
        setConf(context.getConfiguration());
        final QueryPlan queryPlan = getQueryPlan(context);
        try {
            return new PhoenixRecordReader(phoenixConfiguration,queryPlan);    
        }catch(SQLException sqle) {
            throw new IOException(sqle);
        }
    }
    
   

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {        
        List<InputSplit> splits = null;
        try{
            setConf(context.getConfiguration());
            final QueryPlan queryPlan = getQueryPlan(context);
            @SuppressWarnings("unused")
            final ResultIterator iterator = queryPlan.iterator();
            final List<KeyRange> allSplits = queryPlan.getSplits();
            splits = generateSplits(queryPlan,allSplits);
        } catch(SQLException sqlE) {
            LOG.error(String.format(" Error [%s] in getSplits of PhoenixInputFormat ", sqlE.getMessage()));
            Throwables.propagate(sqlE);
        }
        return splits;
    }

    private List<InputSplit> generateSplits(final QueryPlan qplan, final List<KeyRange> splits) throws IOException {
        Preconditions.checkNotNull(qplan);
        Preconditions.checkNotNull(splits);
        final List<InputSplit> psplits = Lists.newArrayListWithExpectedSize(splits.size());
        final StatementContext context = qplan.getContext();
        final TableRef tableRef = qplan.getTableRef();
        for (KeyRange split : splits) {
            final Scan splitScan = new Scan(context.getScan());
            if (tableRef.getTable().getBucketNum() != null) {
                KeyRange minMaxRange = context.getMinMaxRange();
                if (minMaxRange != null) {
                    minMaxRange = SaltingUtil.addSaltByte(split.getLowerRange(), minMaxRange);
                    split = split.intersect(minMaxRange);
                }
            }
            // as the intersect code sets the actual start and stop row within the passed splitScan, we are fetching it back below.
            if (ScanUtil.intersectScanRange(splitScan, split.getLowerRange(), split.getUpperRange(), context.getScanRanges().useSkipScanFilter())) {
                final PhoenixInputSplit inputSplit = new PhoenixInputSplit(KeyRange.getKeyRange(splitScan.getStartRow(), splitScan.getStopRow()));
                psplits.add(inputSplit);     
            }
        }
        return psplits;
    }
    
    public void setConf(Configuration configuration) {
        this.phoenixConfiguration = new PhoenixPigConfiguration(configuration);
    }

    public PhoenixPigConfiguration getConf() {
        return this.phoenixConfiguration;
    }
    
    private Connection getConnection() {
        try {
            if (this.connection == null) {
                this.connection = phoenixConfiguration.getConnection();
           }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return connection;
    }
    
    /**
     * Returns the query plan associated with the select query.
     * @param context
     * @return
     * @throws IOException
     * @throws SQLException
     */
    private QueryPlan getQueryPlan(final JobContext context) throws IOException {
        Preconditions.checkNotNull(context);
        if(queryPlan == null) {
            try{
                final Connection connection = getConnection();
                final String selectStatement = getConf().getSelectStatement();
                Preconditions.checkNotNull(selectStatement);
                final Statement statement = connection.createStatement();
                final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
                this.queryPlan = pstmt.compileQuery(selectStatement);
            } catch(Exception exception) {
                LOG.error(String.format("Failed to get the query plan with error [%s]",exception.getMessage()));
                throw new RuntimeException(exception);
            }
        }
        return queryPlan;
    }
}
